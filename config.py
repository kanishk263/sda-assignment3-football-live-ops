# config.py
# Assignment 3 — Live Match-Day Ops Dashboard (Football, real Kaggle data)
#
# Keep your real MongoDB Atlas URI here ONLY — never commit this file with
# real credentials to a public GitHub repo. Add config.py to .gitignore
# and commit config.py.example instead.

# --- MongoDB Atlas (used for both raw storage AND Atlas Charts) ---
# Replace <user>/<pass> with your real Atlas credentials before running.
MONGO_URI = "mongodb+srv://mongoadmin:mongoadmin140@clusterkanishk.yjelfkr.mongodb.net/"

DB_NAME = "football_live_ops"          # separate DB, not sda_course

RAW_EVENTS_COLLECTION       = "match_events"          # every decoded event, as-is
GOALS_TIMELINE_COLLECTION   = "goals_timeline"        # one doc per goal
CARDS_BY_TEAM_COLLECTION    = "cards_by_team"         # running tally per team
SHOT_OUTCOMES_COLLECTION    = "shot_outcomes"         # running tally per outcome
LIVE_SCOREBOARD_COLLECTION  = "live_scoreboard"       # one doc per match, upserted
EVENT_TYPE_DIST_COLLECTION  = "event_type_distribution"  # running tally per event_type

# --- Kafka ---
KAFKA_BROKER = "localhost:9092"
TOPIC = "football-live-ops-events"

# --- Data source (real Kaggle "football-events" dataset by secareanualin) ---
# Point these at your real files (the ~900,000-row dataset lives on your
# machine). The producer also accepts --events / --ginf on the command
# line, which override these defaults.
EVENTS_CSV = "events.csv"
GINF_CSV = "ginf.csv"

# The matchday is NOT fixed here. Run `python producer.py` with no --date
# flag and it scans ginf.csv, shows you every matchday it finds (with
# match counts and leagues), and lets you browse/pick one interactively.
# --date DD/MM/YY skips the browser for a known date (e.g. automation,
# smoke tests). TARGET_DATE below is only used as a documentation example
# and by generate_sample_data.py's fixture — it plays no role in producer.py.
TARGET_DATE = "06/08/11"   # DD/MM/YY, matches the raw ginf.csv date format

# Real-time pacing: seconds of real time to sleep between each streamed
# event. Kept small since a 90-minute match can have 150-400 raw events.
STREAM_DELAY_SECONDS = 0.4
