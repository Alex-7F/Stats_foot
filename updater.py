# requirements: soccerdata, psycopg2-binary, pandas
import os
import psycopg2
import soccerdata as sd

# Connexion PostgreSQL (via variable secrète GitHub)
conn = psycopg2.connect(os.environ["DATABASE_URL"])
cursor = conn.cursor()

# Initialisation du scraper FBref pour les 5 grands championnats (saison en cours)
# FBref sépare nativement les compétitions officielles
fbref = sd.FBref(leagues=["ENG-Premier League", "ESP-La Liga", "ITA-Serie A", "GER-Bundesliga", "FRA-Ligue 1"], seasons=["2025-2026"])

# Récupération du tableau standard des joueurs (Matchs, Buts, Passes D)
print("Téléchargement des statistiques de la saison...")
season_stats = fbref.read_player_season_stats(stat_type="standard")

# Filtrage : On ne garde que les colonnes indispensables
# FBref indexe par: (league, season, team, player)
for index, row in season_stats.iterrows():
    player_name = index[3]
    team_name = index[2]
    
    # Extraction sécurisée des colonnes
    matches = int(row['Playing Time']['MP']) if ('Playing Time', 'MP') in row else 0
    goals = int(row['Performance']['Gls']) if ('Performance', 'Gls') in row else 0
    assists = int(row['Performance']['Ast']) if ('Performance', 'Ast') in row else 0

    # UPSERT dans PostgreSQL (Mise à jour de la saison en cours)
    query = """
    INSERT INTO player_season_stats (player_name, team_name, season, matches, goals, assists)
    VALUES (%s, %s, '2025-2026', %s, %s, %s)
    ON CONFLICT (player_name, team_name, season)
    DO UPDATE SET 
        matches = EXCLUDED.matches,
        goals = EXCLUDED.goals,
        assists = EXCLUDED.assists;
    """
    cursor.execute(query, (player_name, team_name, matches, goals, assists))

# Recalcul automatique des totaux de carrière dans la table cache
cursor.execute("""
    INSERT INTO player_career_totals (player_name, total_matches, total_goals, total_assists, updated_at)
    SELECT 
        player_name,
        SUM(matches) AS total_matches,
        SUM(goals) AS total_goals,
        SUM(assists) AS total_assists,
        NOW()
    FROM player_season_stats
    GROUP BY player_name
    ON CONFLICT (player_name)
    DO UPDATE SET
        total_matches = EXCLUDED.total_matches,
        total_goals = EXCLUDED.total_goals,
        total_assists = EXCLUDED.total_assists,
        updated_at = NOW();
""")

conn.commit()
cursor.close()
conn.close()
print("Base de données mise à jour avec succès !")