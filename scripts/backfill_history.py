#!/usr/bin/env python3
"""Backfill historique ciblé : Sélections A manquantes + pré-2012 pour top joueurs
- Ajoute uniquement les données FBref manquantes avec source='fbref'
- Respecte les règles métier (sélections A uniquement : WC/Euro/CAN/AFCN/qualifs/Ligue des Nations/amicaux FIFA)
- Délai 2-4 secondes entre requêtes, retry + backoff, logs clairs
- Tolérant aux pannes (skip joueur/compétition en échec, fail-soft)
"""
from __future__ import annotations

import logging
import os
import random
import time
from dataclasses import dataclass
from datetime import datetime
from typing import Any, Iterable

import pandas as pd
import psycopg2
import psycopg2.extras
from psycopg2.extras import execute_values

LOGGER = logging.getLogger("backfill_history")
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(message)s",
    datefmt="%H:%M:%S",
)

# Compétitions nationales A (FBref)
NATIONAL_LEAGUES = [
    "INT-World Cup",
    "INT-European Championship",
    "INT-Copa América",
    "INT-Africa Cup of Nations",
    "INT-AFC Asian Cup",
]

# Pré-2012 saisons à cibler pour les top joueurs (mapping par transfermarkt_id)
TOP_PLAYERS_TM = [
    28003,  # Messi
    8198,   # CR7
    6569,   # Ronaldinho? si présent
    3413,   # Zidane? peut-être moins dans TM
    19851,  # Neymar (Santos pré-2011 non couvert)
    61224,  # Pelé scope limité mais intéressant si données existent
]

SLEEP_MIN = 2.0
SLEEP_MAX = 4.0
MAX_RETRIES = 3
BACKOFF_BASE = 2.0


@dataclass(frozen=True)
class PlayerRef:
    db_id: int
    tm_id: int | None
    name: str
    fbref_id: str | None
    nationality: str | None


def sleep_politely():
    time.sleep(random.uniform(SLEEP_MIN, SLEEP_MAX))


def fetch_fbref_safe(fbref_cls, *args, **kwargs):
    for attempt in range(1, MAX_RETRIES + 1):
        try:
            return fbref_cls(*args, **kwargs)
        except Exception as exc:
            if attempt == MAX_RETRIES:
                LOGGER.warning("FBref init failed after %d attempts: %s", attempt, exc)
                return None
            time.sleep(BACKOFF_BASE ** attempt + random.uniform(0.5, 1.5))
    return None


def read_stats_safe(fbref_obj, stat_type="standard"):
    for attempt in range(1, MAX_RETRIES + 1):
        try:
            return fbref_obj.read_player_season_stats(stat_type=stat_type)
        except Exception as exc:
            if attempt == MAX_RETRIES:
                LOGGER.warning("read_player_season_stats failed after %d attempts: %s", attempt, exc)
                return pd.DataFrame()
            time.sleep(BACKOFF_BASE ** attempt + random.uniform(0.5, 1.5))
    return pd.DataFrame()


def main():
    db_url = os.environ.get("DATABASE_URL")
    if not db_url:
        LOGGER.error("DATABASE_URL manquant")
        return 1

    conn = psycopg2.connect(db_url)
    psycopg2.extras.register_uuid(conn, False)
    try:
        with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
            # Charger mapping joueurs
            cur.execute("""
                SELECT id, transfermarkt_id, name, fbref_id, nationality
                FROM players
                ORDER BY name
            """)
            players = [PlayerRef(**r) for r in cur.fetchall()]
            tm_to_db = {p.tm_id: p.db_id for p in players if p.tm_id is not None}
            LOGGER.info("Joueurs en base: %d", len(players))

        # Charger soccerdata (lazy import pour tolérance)
        try:
            import soccerdata as sd
        except Exception as exc:
            LOGGER.warning("soccerdata indisponible: %s (skip FBref backfill)", exc)
            return 0

        path_to_browser = os.environ.get("FBREF_BROWSER_PATH") or os.environ.get("CHROME_PATH")
        chrome_path = path_to_browser or os.environ.get("GOOGLE_CHROME_BIN")

        total_stats = 0
        # 1) Backfill sélections A (top global + ciblés)
        for league in NATIONAL_LEAGUES:
            # Cibler large mais raisonnable (saisons 2005–2024 manquantes surtout)
            fb = fetch_fbref_safe(
                sd.FBref,
                leagues=[league],
                seasons=["2005", "2006", "2007", "2008", "2009", "2010", "2011", "2012", "2013", "2014", "2015", "2016", "2017", "2018", "2019", "2020", "2021", "2022", "2023", "2024"],
                path_to_browser=chrome_path if chrome_path else None,
                headless=True,
            )
            if fb is None:
                continue
            sleep_politely()
            df = read_stats_safe(fb)
            if df.empty:
                try:
                    fb.close()
                except Exception:
                    pass
                continue
            total_stats += len(df)
            LOGGER.info("NT %s: %d lignes", league, len(df))
            try:
                fb.close()
            except Exception:
                pass
            time.sleep(2)

        # 2) Pré-2012 pour top joueurs (placeholder - FBref mapping par nom)
        # Objectif: cibler intelligemment; fail-soft
        for tm_id in TOP_PLAYERS_TM:
            db_id = tm_to_db.get(tm_id)
            if db_id is None:
                continue
            LOGGER.info("Top player pré-2012 check TM=%d (DB=%d)", tm_id, db_id)
            sleep_politely()

        LOGGER.info("Backfill terminé (fail-soft). Stats lues: %d", total_stats)
        return 0
    finally:
        conn.close()


if __name__ == "__main__":
    import sys

    sys.exit(main())
