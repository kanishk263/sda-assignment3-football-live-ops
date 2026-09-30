# Assignment 3 — Live Match-Day Ops Dashboard (Football)

**Streaming Data Analytics | MBA Course**
Industry: Live Sports Analytics (Football) — carried forward from Assignments 1 & 2.

## What this is

A live-ops dashboard for a broadcaster's match-day control room, built on
**real historical football data** — not a synthetic generator. It replays
one real matchday from the Kaggle **"football-events"** dataset
(secareanualin, ~900,000 event rows across `events.csv` + `ginf.csv`) as a
single interleaved Kafka stream, exactly the way a broadcaster watches
several simultaneous kickoffs on one screen.

This is a legitimate, widely-used technique called **feed replay** —
streaming historical data in chronological order with a pacing delay to
simulate a live feed. Sports-data vendors (Opta, StatsBomb, Sportradar) use
it to test and demo live systems. Nothing about the match content is
invented: every goal, card, shot and substitution below really happened.

## Picking a matchday — nothing is hardcoded

Run `producer.py` with no `--date` flag and it **scans your real
`ginf.csv`, shows you every matchday it actually contains** (with match
count and leagues), and lets you browse and pick one interactively:

```
$ python producer.py
🔎 Scanning events.csv for available matchdays ...
📅 1,428 distinct matchdays found.

--- Matchdays — page 1 of 96 ---
   1) 14/05/17  —  9 match(es)  [E0]
   2) 13/05/17  —  10 match(es) [D1,F1,I1,SP1]
   ...
Type a number to pick that date, 'n'/'p' for next/prev page,
'f <league_code>' to filter (e.g. 'f D1'), 'f' to clear filter, or 'q' to quit.
> f D1
> 1
Stream these 5 match(es) now? [Y/n]
```

Pass `--date DD/MM/YY` to skip the browser when you already know the date
(useful for repeat runs or automation). Every event streamed carries a
`matchday_date` field all the way through to MongoDB, so switching dates
between runs never mixes one day's matches into another's aggregates —
see the Atlas Charts guide for filtering by date on the dashboard.

The specific matchday used to build and smoke-test this pipeline —
**06 Aug 2011**, 5 real fixtures (one Bundesliga + one Ligue 1 round) —
is still what the bundled `sample_data/` fixture reproduces:

| Match | League |
|---|---|
| FC Augsburg vs SC Freiburg | Bundesliga (D1) |
| Werder Bremen vs Kaiserslautern | Bundesliga (D1) |
| Paris Saint-Germain vs Lorient | Ligue 1 (F1) |
| Caen vs Valenciennes | Ligue 1 (F1) |
| Hertha Berlin vs Nurnberg | Bundesliga (D1) |

But with the real files, any date the dataset covers works exactly the
same way.

## Architecture

```
events.csv + ginf.csv  →  producer.py  →  Kafka topic  →  consumer.py  →  MongoDB Atlas  →  Atlas Charts
(real Kaggle data)        (filter,          (interleaved    (decode,        (raw layer +
                            decode,           live stream)    aggregate)      aggregate layer)
                            interleave)
```

- **`legend.py`** — decodes every numeric code in the dataset (event_type,
  shot_place, shot_outcome, location, bodypart, assist_method, situation)
  into human-readable labels, confirmed against real sample rows.
- **`producer.py`** — reads `ginf.csv`, filters to the chosen matchday,
  reads `events.csv` for those 5 matches, decodes every row, sorts all
  events across all 5 matches by minute (interleaving them as if kicking
  off simultaneously), and streams them to Kafka with a small delay for
  live-demo pacing. Tracks a running scoreline per match, correctly
  crediting own goals to the opponent.
- **`consumer.py`** — consumes the stream into MongoDB Atlas:
  - **Raw layer** (`match_events`) — every decoded event, for drill-down.
  - **Aggregate layer**, built for Atlas Charts to read directly:
    - `goals_timeline` — one doc per goal (minute, team, running score)
    - `cards_by_team` — running yellow/red card tally per team
    - `shot_outcomes` — running tally of on target / off target / blocked / hit bar
    - `live_scoreboard` — one doc per match, upserted to the latest score
    - `event_type_distribution` — running tally of every event type seen

## Files

| File | Purpose |
|---|---|
| `legend.py` | Numeric-code → label decoder |
| `producer.py` | Reads real CSVs, interleaves, streams to Kafka |
| `consumer.py` | Consumes stream, writes raw + aggregate collections to Atlas |
| `config.py` | Mongo URI, DB/collection names, Kafka broker/topic, matchday date |
| `generate_sample_data.py` | Builds a small **synthetic** `sample_data/` fixture with the real team names and real final scores, for smoke-testing the pipeline without the full 900k-row files |
| `sample_data/events.csv`, `sample_data/ginf.csv` | Smoke-test fixture (see Notes) |
| `requirements.txt` | `kafka-python`, `pymongo` |

## How to run

1. **Get the real data.** Download `events.csv` + `ginf.csv` from the
   Kaggle "football-events" dataset (secareanualin) and place them in this
   folder (or pass paths with `--events` / `--ginf`).
2. **Start Kafka and MongoDB Atlas.** Kafka runs locally (same Docker setup
   as Assignment 2). MongoDB is **Atlas** this time, not local Docker Mongo
   — Atlas Charts needs a real Atlas cluster to draw from.
3. **Set your Atlas credentials** in `config.py` (`MONGO_URI`).
4. Create the topic (same pattern as Assignment 2):
   ```
   /opt/kafka/bin/kafka-topics.sh --create --topic football-live-ops-events \
     --bootstrap-server localhost:9092 --partitions 1 --replication-factor 1
   ```
5. Run the consumer first (Terminal 1):
   ```
   python consumer.py
   ```
6. Run the producer (Terminal 2) and pick a matchday from the browser:
   ```
   python producer.py --events events.csv --ginf ginf.csv
   ```
   Or skip straight to a known date:
   ```
   python producer.py --events events.csv --ginf ginf.csv --date 06/08/11
   ```
   Or, to smoke-test without the real files:
   ```
   python generate_sample_data.py
   python producer.py --events sample_data/events.csv --ginf sample_data/ginf.csv
   ```
7. **Build the dashboard** in MongoDB Atlas Charts against the aggregate
   collections. See `ATLAS_CHARTS_GUIDE.md` for exact steps and chart
   configurations (goals timeline, cards by team, shot outcomes, live
   scoreboard — 4 charts, exceeding the ≥3 requirement).

## Notes — being upfront about what's real and what's a fixture

- **The event content is 100% real** once you supply the actual
  `events.csv`/`ginf.csv` — every match, goal, card and shot decoded by
  `producer.py` is exactly what happened on the pitch that day.
- **`sample_data/`** is a small synthetic stand-in used only to verify the
  pipeline runs correctly end-to-end without the full 900k-row files. It
  uses the 5 real teams and reproduces the real final scores for that
  matchday exactly, but the minute-by-minute event sequence in between
  (fouls, corners, non-scoring shots) is randomly generated filler, not
  the real event log. This was verified: reconstructing the final score
  from the synthetic event stream matches the real `fthg`/`ftag` for all
  5 matches (see `generate_sample_data.py` and the smoke test in the
  submission notes).
- Swap in the real CSVs and nothing else changes — same producer, same
  consumer, same Atlas Charts dashboards, now backed by real event data.
