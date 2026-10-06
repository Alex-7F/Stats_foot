#!/usr/bin/env python3
"""Updater léger - delta saison active uniquement
- Cible STRICTEMENT la saison en cours (2025-2026 par défaut)
- Mapping FBref -> players via fbref_id puis nom+nationalité
- UPSERT player_season_stats avec source='fbref'
- refresh_career_totals() systématique
- Délai 2-4s, retry + backoff, logs clairs
- FAIL-SOFT en cas de CAPTCHA/indisponibilité FBref (sortie 0)
"""
from __future__ import annotations

import logging
import os
import random
import re
import time
from dataclasses import dataclass
from typing import Any

import pandas as pd
import psycopg2
import psycopg2.extras

LOGGER = logging.getLogger("updater")
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(message)s",
    datefmt="%H:%M:%S",
)

ACTIVE_SEASON = os.environ.get("ACTIVE_SEASON", "2025-2026")
LEAGUES = [
    "ENG-Premier League",
    "ESP-La Liga",
    "ITA-Serie A",
    "GER-Bundesliga",
    "FRA-Ligue 1",
]
SLEEP_MIN = 2.0
SLEEP_MAX = 4.0
MAX_RETRIES = 3
BACKOFF_BASE = 2.0

TM_PREFIX = "https://www.transfermarkt.co.uk/"
FBREF_PREFIX = "https://fbref.com/en/players/"


@dataclass(frozen=True)
class PlayerMap:
    db_id: int
    name: str
    fbref_id: str | None
    tm_id: int | None


def normalize_name(s: str | None) -> str:
    if not s:
        return ""
    s = s.lower()
    s = re.sub(r"[^a-z0-9 ]+", " ", s)
    s = re.sub(r"\s+", " ", s)
    return s.strip()


def sleep_politely():
    time.sleep(random.uniform(SLEEP_MIN, SLEEP_MAX))


def main():
    db_url = os.environ.get("DATABASE_URL")
    if not db_url:
        LOGGER.warning("DATABASE_URL manquant - fail-soft (skip update)")
        return 0

    try:
        import soccerdata as sd
    except Exception as exc:
        LOGGER.warning("soccerdata indisponible: %s - fail-soft (skip update)", exc)
        return 0

    chrome_path = os.environ.get("FBREF_BROWSER_PATH") or os.environ.get("CHROME_PATH") or os.environ.get("GOOGLE_CHROME_BIN")

    # FBref init (retry)
    fb = None
    for attempt in range(1, MAX_RETRIES + 1):
        try:
            fb = sd.FBref(
                leagues=LEAGUES,
                seasons=[ACTIVE_SEASON],
                path_to_browser=chrome_path if chrome_path else None,
                headless=True,
            )
            break
        except Exception as exc:
            if attempt == MAX_RETRIES:
                LOGGER.warning("FBref init échoué après %d tentatives: %s - fail-soft (skip update)", attempt, exc)
                return 0
            time.sleep(BACKOFF_BASE ** attempt + random.uniform(0.5, 1.0))
    if fb is None:
        return 0

    # Stats
    df = pd.DataFrame()
    for attempt in range(1, MAX_RETRIES + 1):
        try:
            sleep_politely()
            df = fb.read_player_season_stats(stat_type="standard")
            break
        except Exception as exc:
            if attempt == MAX_RETRIES:
                LOGGER.warning("read_player_season_stats échoué: %s - fail-soft (skip update)", exc)
                try:
                    fb.close()
                except Exception:
                    pass
                return 0
            time.sleep(BACKOFF_BASE ** attempt + random.uniform(1.0, 2.0))

    try:
        fb.close()
    except Exception:
        pass

    if df.empty:
        LOGGER.info("FBref: aucune donnée renvoyée pour %s - skip", ACTIVE_SEASON)
        return 0

    LOGGER.info("FBref %s: %d lignes lues", ACTIVE_SEASON, len(df))

    # Connexion DB
    try:
        conn = psycopg2.connect(db_url)
        psycopg2.extras.register_uuid(conn, False)
    except Exception as exc:
        LOGGER.warning("Connexion DB impossible: %s - fail-soft", exc)
        return 0

    try:
        with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
            cur.execute("""
                SELECT id, transfermarkt_id, name, fbref_id, nationality
                FROM players
            """)
            players = [PlayerMap(**r) for r in cur.fetchall()]

        # mapping
        fbref_to_db = {p.fbref_id: p.db_id for p in players if p.fbref_id}
        name_norm_to_db = {}
        for p in players:
            key = normalize_name(p.name)
            if key:
                # keep first or prefer non-None? not critical
                if key not in name_norm_to_db:
                    name_norm_to_db[key] = p.db_id

        rows = []
        matched = 0
        unmatched = 0
        for idx, row in df.iterrows():
            try:
                league = idx[0] if len(idx) > 0 else ""
                season_fbref = idx[1] if len(idx) > 1 else ACTIVE_SEASON
                team_name = idx[2] if len(idx) > 2 else ""
                player_name = idx[3] if len(idx) > 3 else ""
            except Exception:
                continue

            # fbref_id peut être dans colonnes? pas garanti
            fbref_id = None
            try:
                if "fbref_id" in row:
                    fbref_id = str(row["fbref_id"]) if pd.notna(row["fbref_id"]) else None
            except Exception:
                pass

            db_id = None
            if fbref_id and fbref_id in fbref_to_db:
                db_id = fbref_to_db[fbref_id]
                matched += 1
            else:
                nkey = normalize_name(player_name)
                if nkey in name_norm_to_db:
                    db_id = name_norm_to_db[nkey]
                    matched += 1
                else:
                    unmatched += 1
                    continue

            # extraire stats multi-index
            try:
                matches = int(row[('Playing Time', 'MP')]) if ('Playing Time', 'MP') in row else int(row.get('MP', 0)) if 'MP' in row else 0
            except Exception:
                try:
                    matches = int(row['Playing Time']['MP']) if hasattr(row['Playing Time'], '__getitem__') else 0
                except Exception:
                    matches = 0
            try:
                goals = int(row[('Performance', 'Gls')]) if ('Performance', 'Gls') in row else int(row.get('Gls', 0)) if 'Gls' in row else 0
            except Exception:
                goals = 0
            try:
                assists = int(row[('Performance', 'Ast')]) if ('Performance', 'Ast') in row else int(row.get('Ast', 0)) if 'Ast' in row else 0
            except Exception:
                assists = 0

            season_int = int(str(season_fbref).split("-")[0]) if "-" in str(season_fbref) else int(season_fbref) if pd.notna(season_fbref) else int(ACTIVE_SEASON.split("-")[0])

            # CLUB par défaut pour ligues majeures; NATIONAL à gérer via autre source si besoin
            category = "CLUB"
            competition_type = "league"
            rows.append((db_id, season_int, team_name, category, competition_type, matches, goals, assists))

        LOGGER.info("Mapping: matched=%d unmatched=%d -> à insérer=%d", matched, unmatched, len(rows))
        if not rows:
            LOGGER.info("Rien à insérer - exit")
            return 0

        with conn.cursor() as cur:
            # Supprimer uniquement lignes fbref de cette saison CLUB ciblée (nettoyant)
            cur.execute(
                """
                DELETE FROM player_season_stats
                WHERE source = 'fbref'
                  AND season = %s
                  AND category = 'CLUB'
                """,
                (int(ACTIVE_SEASON.split("-")[0]),),
            )
            psycopg2.extras.execute_values(
                cur,
                """
                INSERT INTO player_season_stats (
                    player_id, season, team_name, category, competition_type,
                    matches, goals, assists, source
                ) VALUES %s
                ON CONFLICT (player_id, season, team_name, category, competition_type)
                DO UPDATE SET
                    matches = EXCLUDED.matches,
                    goals = EXCLUDED.goals,
                    assists = EXCLUDED.assists,
                    source = 'fbref'
                """,
                [(r[0], r[1], r[2], r[3], r[4], r[5], r[6], r[7], 'fbref') for r in rows],
                page_size=2000,
            )
            cur.execute("SELECT refresh_career_totals()")
        conn.commit()
        LOGGER.info("Mise à jour réussie pour %s (%d lignes)", ACTIVE_SEASON, len(rows))
        return 0
    except Exception as exc:
        LOGGER.warning("Erreur non critique: %s - fail-soft", exc)
        try:
            conn.rollback()
        except Exception:
            pass
        return 0
    finally:
        try:
            conn.close()
        except Exception:
            pass


if __name__ == "__main__":
    import sys

    sys.exit(main())
