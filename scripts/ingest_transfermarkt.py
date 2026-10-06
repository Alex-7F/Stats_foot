#!/usr/bin/env python3
"""Ingestion initiale : transfermarkt-datasets -> Supabase (PostgreSQL).

Charge le dump open source transfermarkt-datasets (5 CSV gzip), applique les
règles métier « matchs officiels uniquement », agrège par joueur / saison /
équipe / type de compétition, puis recalcule le cache `player_career_totals`.

Usage :
    export DATABASE_URL="postgresql://user:pass@host:5432/db?sslmode=require"
    python scripts/ingest_transfermarkt.py --data-dir ./data/transfermarkt

Le script est idempotent : il remplace toutes les lignes de source
'transfermarkt' (les lignes 'fbref' ajoutées plus tard sont préservées) et
réexécuter un import partiellement échoué est sans risque.
"""
from __future__ import annotations

import argparse
import logging
import os
import re
import sys
import time
import unicodedata
from dataclasses import dataclass
from datetime import date
from pathlib import Path
from typing import Any, Final

import pandas as pd
import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

LOGGER = logging.getLogger("ingest")

DATASET_BASE_URL: Final = "https://pub-e682421888d945d684bcae8890b0ec20.r2.dev/data"
DATA_FILES: Final[tuple[str, ...]] = (
    "players.csv.gz",
    "appearances.csv.gz",
    "games.csv.gz",
    "competitions.csv.gz",
    "clubs.csv.gz",
)

# Compétitions présentes dans games.csv mais absentes de competitions.csv.
# Vérifiées manuellement : toutes sont des compétitions officielles sénior.
ORPHAN_COMPETITIONS: Final[dict[str, tuple[str, str]]] = {
    "CGB": ("CLUB", "cup"),         # Coupe de la Ligue (Angleterre)
    "POCP": ("CLUB", "cup"),        # Taça da Liga (Portugal)
    "UKRS": ("CLUB", "super_cup"),  # Supercoupe d'Ukraine
    "KLUB": ("CLUB", "other"),      # Coupe du Monde des Clubs (FIFA)
    "COL1": ("CLUB", "league"),     # Liga BetPlay (Colombie)
}

# Hors règles métier, quelle que soit la catégorie : sélections jeunes,
# Jeux Olympiques, réserves. (Les amicaux de club sont gérés à part :
# amicaux de sélection = autorisés.)
FORBIDDEN_COMPETITION_KEYWORDS: Final[tuple[str, ...]] = (
    "u17", "u19", "u20", "u21", "u23",
    "olymp", "youth", "junior", "reserv",
)

# Amicaux de club : INTERDITS par les règles métier.
CLUB_FRIENDLY_KEYWORDS: Final[tuple[str, ...]] = (
    "friendly", "testspiel", "amical", "amichevole",
)


@dataclass(frozen=True)
class CompetitionClass:
    category: str          # 'CLUB' | 'NATIONAL'
    competition_type: str  # vocabulaire contrôlé par le schéma SQL


def _str_or_none(value: Any) -> str | None:
    if isinstance(value, str) and value.strip():
        return value.strip()
    return None


# ---------------------------------------------------------------------------
# Classification des compétitions (règles métier)
# ---------------------------------------------------------------------------

def _classify_national(sub_type: str, comp_type: str) -> CompetitionClass | None:
    if "qualif" in sub_type:
        return CompetitionClass("NATIONAL", "qualifier")
    if "nations_league" in sub_type or "nations league" in sub_type:
        return CompetitionClass("NATIONAL", "nations_league")
    if "friendly" in sub_type or "freundschaft" in sub_type:
        return CompetitionClass("NATIONAL", "friendly")
    if comp_type == "national_team_competition":
        return CompetitionClass("NATIONAL", "tournament")
    return None


def _classify_club(sub_type: str, comp_type: str) -> CompetitionClass | None:
    if "tier" in sub_type:  # first_tier, second_tier, ...
        return CompetitionClass("CLUB", "league")
    if "play_off" in sub_type:
        return CompetitionClass("CLUB", "playoff")
    if "super_cup" in sub_type:
        return CompetitionClass("CLUB", "super_cup")
    if sub_type.startswith("uefa_") or comp_type == "international_cup":
        return CompetitionClass("CLUB", "european_cup")
    if "cup" in sub_type or comp_type == "domestic_cup":
        return CompetitionClass("CLUB", "cup")
    if comp_type == "domestic_league":
        return CompetitionClass("CLUB", "league")
    if comp_type == "other":
        return CompetitionClass("CLUB", "other")
    return None


def classify_competitions(
    competitions: pd.DataFrame,
    games: pd.DataFrame,
) -> tuple[dict[str, CompetitionClass], dict[str, int]]:
    """Retourne la classification par competition_id + compteurs d'exclusions."""
    classes: dict[str, CompetitionClass] = {}
    counters = {"amicaux_club": 0, "interdits": 0, "inconnues": 0}

    for comp_id, sub_type, comp_type, name in competitions[
        ["competition_id", "sub_type", "type", "name"]
    ].itertuples(index=False):
        sub = (sub_type or "").lower()
        ctype = (comp_type or "").lower()
        comp_name = (name or "").lower()
        haystack = f"{comp_name} {sub}"

        if any(keyword in haystack for keyword in FORBIDDEN_COMPETITION_KEYWORDS):
            counters["interdits"] += 1
            continue

        if ctype == "national_team_competition":
            classification = _classify_national(sub, ctype)
        else:
            if any(keyword in haystack for keyword in CLUB_FRIENDLY_KEYWORDS):
                counters["amicaux_club"] += 1
                continue
            classification = _classify_club(sub, ctype)

        if classification is None:
            counters["inconnues"] += 1
            LOGGER.warning("Compétition non classée : %s (%s / %s)", comp_id, sub_type, comp_type)
            continue
        classes[comp_id] = classification

    # Compétitions utilisées dans games.csv mais absentes du catalogue
    game_comps = set(games["competition_id"].dropna().unique())
    for comp_id in sorted(game_comps - set(classes)):
        override = ORPHAN_COMPETITIONS.get(comp_id)
        if override is None:
            counters["inconnues"] += 1
            LOGGER.warning("Compétition orpheline non répertoriée, exclue : %s", comp_id)
            continue
        classes[comp_id] = CompetitionClass(*override)

    return classes, counters


# ---------------------------------------------------------------------------
# Chargement & transformation
# ---------------------------------------------------------------------------

def download_dataset(data_dir: Path) -> None:
    session = requests.Session()
    retry = Retry(
        total=5,
        backoff_factor=2.0,
        status_forcelist=(429, 500, 502, 503, 504),
        allowed_methods=("GET",),
    )
    session.mount("https://", HTTPAdapter(max_retries=retry))

    for filename in DATA_FILES:
        target = data_dir / filename
        if target.exists():
            LOGGER.info("Présent dans le cache : %s", filename)
            continue
        url = f"{DATASET_BASE_URL}/{filename}"
        partial = target.with_suffix(target.suffix + ".part")
        LOGGER.info("Téléchargement de %s ...", filename)
        with session.get(url, stream=True, timeout=(30, 300)) as response:
            response.raise_for_status()
            with open(partial, "wb") as handle:
                for chunk in response.iter_content(chunk_size=1 << 20):
                    handle.write(chunk)
        partial.replace(target)


def load_frames(data_dir: Path) -> dict[str, pd.DataFrame]:
    frames = {
        "players": pd.read_csv(
            data_dir / "players.csv.gz",
            usecols=["player_id", "first_name", "last_name", "name", "player_code",
                     "date_of_birth", "country_of_citizenship", "image_url", "last_season"],
            dtype=str,
        ),
        "appearances": pd.read_csv(
            data_dir / "appearances.csv.gz",
            usecols=["game_id", "player_id", "player_club_id", "competition_id",
                     "goals", "assists"],
            dtype=str,
        ),
        "games": pd.read_csv(
            data_dir / "games.csv.gz",
            usecols=["game_id", "competition_id", "season", "home_club_id", "away_club_id",
                     "home_club_name", "away_club_name"],
            dtype=str,
        ),
        "competitions": pd.read_csv(
            data_dir / "competitions.csv.gz",
            usecols=["competition_id", "name", "sub_type", "type"],
            dtype=str,
        ),
        "clubs": pd.read_csv(data_dir / "clubs.csv.gz", usecols=["club_id", "name"], dtype=str),
    }
    for name, frame in frames.items():
        LOGGER.info("Chargé : %s lignes=%d", name, len(frame))
    return frames


def build_season_slices(
    frames: dict[str, pd.DataFrame],
    classes: dict[str, CompetitionClass],
) -> tuple[pd.DataFrame, pd.DataFrame, dict[str, int]]:
    """Agrège les apparitions en tranches (joueur, saison, équipe, compétition).

    Retourne (tranches, joueurs concernés, compteurs qualité).
    """
    games = frames["games"].drop_duplicates("game_id")
    apps = frames["appearances"]
    clubs = frames["clubs"].set_index("club_id")["name"]
    quality = {"sans_match": 0, "competition_incoherente": 0, "sans_joueur": 0,
               "sans_equipe": 0, "sans_saison": 0, "competition_inconnue": 0}

    merged = apps.merge(
        games, on="game_id", how="left", suffixes=("_app", ""), validate="many_to_one",
    )

    missing_game = merged["season"].isna()
    quality["sans_match"] = int(missing_game.sum())
    merged = merged.loc[~missing_game].copy()

    mismatch = merged["competition_id_app"] != merged["competition_id"]
    quality["competition_incoherente"] = int(mismatch.sum())
    merged = merged.loc[~mismatch].copy()

    unclassified = ~merged["competition_id"].isin(classes)
    quality["competition_inconnue"] = int(unclassified.sum())
    merged = merged.loc[~unclassified].copy()

    merged["season"] = pd.to_numeric(merged["season"], errors="coerce").astype("Int16")
    missing_season = merged["season"].isna()
    quality["sans_saison"] = int(missing_season.sum())
    merged = merged.loc[~missing_season].copy()

    merged["category"] = merged["competition_id"].map(
        {cid: c.category for cid, c in classes.items()}
    )
    merged["competition_type"] = merged["competition_id"].map(
        {cid: c.competition_type for cid, c in classes.items()}
    )

    # Équipe : nom du club si connu de clubs.csv, sinon nom du côté du terrain
    # (les sélections n'existent pas dans clubs.csv : l'id côté est l'équipe).
    side_name = merged["home_club_name"].where(
        merged["player_club_id"].eq(merged["home_club_id"]),
        merged["away_club_name"].where(
            merged["player_club_id"].eq(merged["away_club_id"]),
            pd.NA,
        ),
    )
    merged["team_name"] = merged["player_club_id"].map(clubs).fillna(side_name)
    missing_team = merged["team_name"].isna()
    quality["sans_equipe"] = int(missing_team.sum())
    merged = merged.loc[~missing_team].copy()

    known_players = set(frames["players"]["player_id"])
    unknown_player = ~merged["player_id"].isin(known_players)
    quality["sans_joueur"] = int(unknown_player.sum())
    merged = merged.loc[~unknown_player].copy()

    merged = merged.drop_duplicates(subset=["game_id", "player_id"])
    merged["goals"] = pd.to_numeric(merged["goals"], errors="coerce").fillna(0).astype("int32")
    merged["assists"] = pd.to_numeric(merged["assists"], errors="coerce").fillna(0).astype("int32")

    slices = (
        merged.groupby(
            ["player_id", "season", "team_name", "category", "competition_type"],
            as_index=False,
            sort=False,
        )
        .agg(matches=("game_id", "count"), goals=("goals", "sum"), assists=("assists", "sum"))
    )
    slices["season"] = slices["season"].astype("int16")
    slices["matches"] = slices["matches"].astype("int32")
    slices["goals"] = slices["goals"].astype("int32")
    slices["assists"] = slices["assists"].astype("int32")

    players = frames["players"].loc[
        frames["players"]["player_id"].isin(slices["player_id"])
    ].copy()

    LOGGER.info(
        "Agrégation : %d tranches, %d joueurs, %d matchs, %d buts, %d passes",
        len(slices), len(players), int(slices["matches"].sum()),
        int(slices["goals"].sum()), int(slices["assists"].sum()),
    )
    return slices, players, quality


def _slugify(value: str) -> str:
    ascii_value = unicodedata.normalize("NFKD", value).encode("ascii", "ignore").decode()
    return re.sub(r"[^a-z0-9]+", "-", ascii_value.lower()).strip("-")


def _active_season(today: date) -> int:
    """Année de début de la saison en cours (début européen en août)."""
    return today.year if today.month >= 7 else today.year - 1


def prepare_players(players: pd.DataFrame) -> list[tuple[Any, ...]]:
    active_season = _active_season(date.today())
    slugs: set[str] = set()
    rows: list[tuple[Any, ...]] = []

    for row in players.sort_values("player_id").itertuples(index=False):
        display_name = _str_or_none(row.name) or f"Joueur {row.player_id}"
        raw_slug = _str_or_none(row.player_code) or _slugify(display_name) or row.player_id
        slug = raw_slug if raw_slug not in slugs else f"{raw_slug}-{row.player_id}"
        slugs.add(slug)

        birth_date = pd.to_datetime(row.date_of_birth, errors="coerce")
        last_season = pd.to_numeric(row.last_season, errors="coerce")
        last_season_int = int(last_season) if pd.notna(last_season) else None
        is_active = last_season_int is not None and last_season_int >= active_season - 1

        rows.append((
            int(row.player_id),      # transfermarkt_id
            slug,
            display_name,
            _str_or_none(row.first_name),
            _str_or_none(row.last_name),
            birth_date.date() if pd.notna(birth_date) else None,
            _str_or_none(row.country_of_citizenship),
            _str_or_none(row.image_url),
            last_season_int,
            is_active,
        ))
    return rows


# ---------------------------------------------------------------------------
# Écriture PostgreSQL
# ---------------------------------------------------------------------------

def connect(database_url: str):  # import tardif : --dry-run sans psycopg2
    import psycopg2

    last_error: Exception | None = None
    for attempt in range(1, 4):
        try:
            return psycopg2.connect(database_url)
        except psycopg2.OperationalError as error:
            last_error = error
            LOGGER.warning("Connexion impossible (tentative %d/3) : %s", attempt, error)
            time.sleep(2 ** attempt)
    raise RuntimeError(f"Connexion PostgreSQL impossible : {last_error}")


def write_to_database(
    database_url: str,
    player_rows: list[tuple[Any, ...]],
    slices: pd.DataFrame,
) -> None:
    from psycopg2.extras import execute_values

    started = time.perf_counter()

    # Les identifiants joueurs du DataFrame sont des str (CSV lu en dtype=str)
    # alors que transfermarkt_id est un bigint : sans conversion, la clé de
    # tm_to_db (int) ne matche jamais le set player_id (str).
    slices = slices.copy()
    slices["player_id"] = slices["player_id"].astype("int64")

    conn = connect(database_url)
    try:
        with conn.cursor() as cursor:
            execute_values(
                cursor,
                """
                insert into players (
                    transfermarkt_id, slug, name, first_name, last_name,
                    birth_date, nationality, photo_url, last_season, is_active
                ) values %s
                on conflict (transfermarkt_id) do update set
                    slug = excluded.slug,
                    name = excluded.name,
                    first_name = excluded.first_name,
                    last_name = excluded.last_name,
                    birth_date = excluded.birth_date,
                    nationality = excluded.nationality,
                    photo_url = excluded.photo_url,
                    last_season = excluded.last_season,
                    is_active = excluded.is_active
                """,
                player_rows,
                page_size=2000,
            )
            LOGGER.info("players upsert : %d lignes", len(player_rows))

            cursor.execute("select transfermarkt_id, id from players")
            tm_to_db = {int(tm_id): int(db_id) for tm_id, db_id in cursor.fetchall()}
            LOGGER.info("Joueurs présents dans la table players : %d", len(tm_to_db))
            missing = {int(pid) for pid in slices["player_id"]} - set(tm_to_db)
            if missing:
                sample = sorted(missing)[:10]
                raise RuntimeError(
                    f"{len(missing)} joueurs absents de la table players (ex. {sample})"
                )

            # Remplacement complet des tranches transfermarkt (idempotent,
            # préserve les lignes fbref éventuelles).
            cursor.execute("delete from player_season_stats where source = 'transfermarkt'")

            payload = [
                (
                    tm_to_db[int(player_id)], int(season), team_name, category,
                    competition_type, int(matches), int(goals), int(assists),
                    "transfermarkt",
                )
                for player_id, season, team_name, category, competition_type,
                matches, goals, assists in slices.itertuples(index=False)
            ]
            execute_values(
                cursor,
                """
                insert into player_season_stats (
                    player_id, season, team_name, category, competition_type,
                    matches, goals, assists, source
                ) values %s
                on conflict (player_id, season, team_name, category, competition_type)
                do update set
                    matches = excluded.matches,
                    goals = excluded.goals,
                    assists = excluded.assists,
                    source = excluded.source
                """,
                payload,
                page_size=2000,
            )
            LOGGER.info("player_season_stats remplacé : %d tranches", len(payload))

            cursor.execute("select refresh_career_totals()")

        conn.commit()
        LOGGER.info("Écriture terminée en %.1fs", time.perf_counter() - started)
    except Exception:
        conn.rollback()
        raise
    finally:
        conn.close()


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--data-dir", type=Path, default=Path("data/transfermarkt"),
        help="Dossier de cache des CSV transfermarkt-datasets",
    )
    parser.add_argument(
        "--skip-download", action="store_true",
        help="Utiliser les fichiers déjà présents dans --data-dir",
    )
    parser.add_argument(
        "--dry-run", action="store_true",
        help="Tout calculer et afficher un rapport sans toucher à la base",
    )
    parser.add_argument(
        "--database-url", default=None,
        help="Sinon variable d'environnement DATABASE_URL",
    )
    parser.add_argument("--log-level", default="INFO")
    return parser.parse_args(argv)


def main(argv: list[str] | None = None) -> int:
    args = parse_args(argv)
    logging.basicConfig(
        level=getattr(logging, args.log_level.upper(), logging.INFO),
        format="%(asctime)s %(levelname)s %(message)s",
        datefmt="%H:%M:%S",
    )
    started = time.perf_counter()

    args.data_dir.mkdir(parents=True, exist_ok=True)
    if not args.skip_download:
        download_dataset(args.data_dir)

    frames = load_frames(args.data_dir)
    classes, exclusions = classify_competitions(frames["competitions"], frames["games"])
    LOGGER.info("Compétitions classées=%d exclusions=%s", len(classes), exclusions)

    slices, players, quality = build_season_slices(frames, classes)
    LOGGER.info("Qualité des données : %s", quality)

    if slices.empty:
        LOGGER.error("Aucune statistique produite : arrêt.")
        return 1

    player_rows = prepare_players(players)
    LOGGER.info("Joueurs préparés : %d", len(player_rows))

    if args.dry_run:
        totals = slices.groupby("category")[["matches", "goals", "assists"]].sum()
        LOGGER.info("Dry-run — totaux par catégorie :\n%s", totals.to_string())
        sample = slices.loc[slices["player_id"].isin(["28003", "342229"])]
        LOGGER.info(
            "Dry-run — échantillon Messi (28003) / Mbappé (342229) :\n%s",
            sample.head(12).to_string(index=False),
        )
    else:
        database_url = args.database_url or os.environ.get("DATABASE_URL")
        if not database_url:
            LOGGER.error("DATABASE_URL manquante (--database-url ou variable d'environnement).")
            return 1
        write_to_database(database_url, player_rows, slices)

    LOGGER.info("Terminé en %.1fs", time.perf_counter() - started)
    return 0


if __name__ == "__main__":
    sys.exit(main())
