"""
consumer.py
Assignment 3 — Live Match-Day Ops Dashboard (Football)
Streaming Data Analytics | MBA Course

Consumes the interleaved matchday event stream produced by producer.py and
writes it into MongoDB Atlas in two layers:

  1. Raw layer  — every decoded event, stored as-is, in `match_events`.
     This is the audit trail / drill-down layer.

  2. Aggregate layer — purpose-built collections that MongoDB Atlas
     Charts reads directly, so charts don't have to run heavy
     aggregation pipelines live:
       - goals_timeline        one doc per goal, scored minute + running score
       - cards_by_team         running tally of yellow/red cards per team
       - shot_outcomes         running tally of shot outcomes (on/off/blocked)
       - live_scoreboard       one doc per match, upserted to the latest score
       - event_type_distribution   running tally of every event type seen

UPDATE config.MONGO_URI with your real Atlas connection string before
running. Uses the same `clusterkanishk` cluster already set up for the
Lecture 10 fraud pipeline, but a separate database (football_live_ops).
"""

import json
from datetime import datetime, timezone

from kafka import KafkaConsumer
from pymongo import MongoClient

import config

# ---------------------------------------------------------------------------
# Connect
# ---------------------------------------------------------------------------
client = MongoClient(config.MONGO_URI)
db = client[config.DB_NAME]

raw_events    = db[config.RAW_EVENTS_COLLECTION]
goals         = db[config.GOALS_TIMELINE_COLLECTION]
cards         = db[config.CARDS_BY_TEAM_COLLECTION]
shots         = db[config.SHOT_OUTCOMES_COLLECTION]
scoreboard    = db[config.LIVE_SCOREBOARD_COLLECTION]
event_dist    = db[config.EVENT_TYPE_DIST_COLLECTION]

consumer = KafkaConsumer(
    config.TOPIC,
    bootstrap_servers=[config.KAFKA_BROKER],
    auto_offset_reset="earliest",
    group_id="football-live-ops-group",
    value_deserializer=lambda v: json.loads(v.decode("utf-8")),
)

print("📡 Live Match-Day Ops Consumer started")
print(f"   Mongo DB   : {config.DB_NAME}  (Atlas cluster)")
print(f"   Kafka topic: {config.TOPIC}")
print("=" * 70)


def now_utc():
    return datetime.now(timezone.utc)


def bump_card_tally(team, card_type, matchday_date):
    cards.update_one(
        {"team": team, "matchday_date": matchday_date},
        {
            "$inc": {f"tally.{card_type}": 1},
            "$set": {"last_updated": now_utc()},
        },
        upsert=True,
    )


def bump_shot_tally(outcome, matchday_date):
    if not outcome:
        return
    shots.update_one(
        {"outcome": outcome, "matchday_date": matchday_date},
        {"$inc": {"count": 1}, "$set": {"last_updated": now_utc()}},
        upsert=True,
    )


def bump_event_type_tally(event_type, matchday_date):
    event_dist.update_one(
        {"event_type": event_type, "matchday_date": matchday_date},
        {"$inc": {"count": 1}, "$set": {"last_updated": now_utc()}},
        upsert=True,
    )


def upsert_scoreboard(payload):
    scoreboard.update_one(
        {"match_id": payload["match_id"]},
        {
            "$set": {
                "match_id": payload["match_id"],
                "home_team": payload["home_team"],
                "away_team": payload["away_team"],
                "league": payload.get("league"),
                "country": payload.get("country"),
                "matchday_date": payload.get("matchday_date"),
                "score_home": payload["score_home"],
                "score_away": payload["score_away"],
                "last_minute": payload["minute"],
                "last_updated": now_utc(),
            }
        },
        upsert=True,
    )


def handle_matchday_complete(payload):
    print("\n🏁 MATCHDAY COMPLETE received — final scores:")
    for m in payload["matches"]:
        print(f"    {m['home_team']} {m['score_home']} - {m['score_away']} {m['away_team']}")
    for m in payload["matches"]:
        scoreboard.update_one(
            {"match_id": m["match_id"]},
            {"$set": {**m, "final": True, "last_updated": now_utc()}},
            upsert=True,
        )


count = 0
for msg in consumer:
    payload = msg.value

    if payload.get("event_type") == "MATCHDAY_COMPLETE":
        handle_matchday_complete(payload)
        continue

    count += 1

    # 1) raw layer — store every decoded event as-is
    raw_events.insert_one({**payload, "ingested_at": now_utc()})

    # 2) aggregate layer, tailored to Atlas Charts
    upsert_scoreboard(payload)
    bump_event_type_tally(payload.get("event_type"), payload.get("matchday_date"))

    if payload.get("is_goal"):
        goals.insert_one({
            "match_id": payload["match_id"],
            "matchday_date": payload.get("matchday_date"),
            "match_label": payload.get("match_label"),
            "minute": payload["minute"],
            "scoring_team": payload.get("event_team"),
            "player": payload.get("player"),
            "own_goal": payload.get("event_type2") == "Own goal",
            "score_home": payload["score_home"],
            "score_away": payload["score_away"],
            "recorded_at": now_utc(),
        })

    if payload.get("event_type") in ("Yellow card", "Second yellow card", "Red card"):
        bump_card_tally(payload.get("event_team"), payload["event_type"], payload.get("matchday_date"))

    if payload.get("event_type") == "Attempt":
        bump_shot_tally(payload.get("shot_outcome"), payload.get("matchday_date"))

    if count % 25 == 0:
        print(f"   ...{count} events stored")

print(f"\n✅ Stream ended. Total events stored: {count}")
