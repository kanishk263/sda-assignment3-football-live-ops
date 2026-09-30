"""
build_insights.py
Assignment 3 — Live Match-Day Ops Dashboard (Football)

Run this AFTER producer.py + consumer.py have streamed at least one
matchday (or at any point while raw events already sit in `match_events`).
It runs real MongoDB aggregation pipelines over the raw event layer to
produce three ANALYTICAL collections — not running tallies, but the kind
of derived metrics an actual match-day analytics team would want:

  1. team_shot_efficiency   — shots, shots-on-target %, goals, and
                               conversion rate % per team. Answers
                               "who is actually clinical in front of
                               goal, not just who shoots the most."

  2. home_away_comparison   — goals / cards / fouls / shots / shots-on-
                               -target, split Home vs Away across the
                               whole matchday. Tests whether "home
                               advantage" (more attacking output, fewer
                               cards for the home side) shows up in the
                               real data.

  3. goal_methods           — every goal broken down by body part
                               (right foot / left foot / head) crossed
                               with situation (open play / set piece /
                               corner / free kick). Answers "how are
                               teams actually scoring."

These are FULL recomputations each run — safe to re-run any time. Each
collection is cleared and rebuilt from `match_events`, because these are
read-only analytical views over the raw layer, not part of the live
streaming path itself (that's still producer.py -> Kafka -> consumer.py).

Usage:
    python build_insights.py
"""

from pymongo import MongoClient

import config

client = MongoClient(config.MONGO_URI)
db = client[config.DB_NAME]
raw = db[config.RAW_EVENTS_COLLECTION]

CARD_TYPES = ["Yellow card", "Second yellow card", "Red card"]


def rebuild_team_shot_efficiency():
    pipeline = [
        {"$match": {"event_type": "Attempt"}},
        {"$group": {
            "_id": {"team": "$event_team", "matchday_date": "$matchday_date"},
            "shots_total": {"$sum": 1},
            "shots_on_target": {"$sum": {"$cond": [{"$eq": ["$shot_outcome", "On target"]}, 1, 0]}},
            "goals": {"$sum": {"$cond": [{"$eq": ["$is_goal", True]}, 1, 0]}},
        }},
        {"$project": {
            "_id": 0,
            "team": "$_id.team",
            "matchday_date": "$_id.matchday_date",
            "shots_total": 1,
            "shots_on_target": 1,
            "goals": 1,
            "conversion_rate_pct": {
                "$cond": [{"$eq": ["$shots_total", 0]}, 0,
                          {"$round": [{"$multiply": [{"$divide": ["$goals", "$shots_total"]}, 100]}, 1]}]
            },
            "on_target_rate_pct": {
                "$cond": [{"$eq": ["$shots_total", 0]}, 0,
                          {"$round": [{"$multiply": [{"$divide": ["$shots_on_target", "$shots_total"]}, 100]}, 1]}]
            },
        }},
        {"$sort": {"conversion_rate_pct": -1}},
    ]
    results = list(raw.aggregate(pipeline))
    coll = db["team_shot_efficiency"]
    coll.delete_many({})
    if results:
        coll.insert_many(results)
    print(f"✅ team_shot_efficiency: {len(results)} teams")
    return results


def rebuild_home_away_comparison():
    pipeline = [
        {"$match": {"side": {"$in": ["Home", "Away"]}}},
        {"$group": {
            "_id": {"side": "$side", "matchday_date": "$matchday_date"},
            "goals": {"$sum": {"$cond": [{"$eq": ["$is_goal", True]}, 1, 0]}},
            "cards": {"$sum": {"$cond": [{"$in": ["$event_type", CARD_TYPES]}, 1, 0]}},
            "fouls": {"$sum": {"$cond": [{"$eq": ["$event_type", "Foul"]}, 1, 0]}},
            "shots": {"$sum": {"$cond": [{"$eq": ["$event_type", "Attempt"]}, 1, 0]}},
            "shots_on_target": {"$sum": {"$cond": [
                {"$and": [{"$eq": ["$event_type", "Attempt"]}, {"$eq": ["$shot_outcome", "On target"]}]}, 1, 0
            ]}},
        }},
        {"$project": {
            "_id": 0, "side": "$_id.side", "matchday_date": "$_id.matchday_date",
            "goals": 1, "cards": 1, "fouls": 1, "shots": 1, "shots_on_target": 1,
        }},
        {"$sort": {"matchday_date": 1, "side": 1}},
    ]
    results = list(raw.aggregate(pipeline))
    coll = db["home_away_comparison"]
    coll.delete_many({})
    if results:
        coll.insert_many(results)
    print(f"✅ home_away_comparison: {len(results)} rows")
    return results


def rebuild_goal_methods():
    pipeline = [
        {"$match": {"is_goal": True}},
        {"$group": {
            "_id": {
                "bodypart": {"$ifNull": ["$bodypart", "Unknown"]},
                "situation": {"$ifNull": ["$situation", "Unknown"]},
                "matchday_date": "$matchday_date",
            },
            "count": {"$sum": 1},
        }},
        {"$project": {
            "_id": 0, "bodypart": "$_id.bodypart", "situation": "$_id.situation",
            "matchday_date": "$_id.matchday_date", "count": 1,
        }},
        {"$sort": {"count": -1}},
    ]
    results = list(raw.aggregate(pipeline))
    coll = db["goal_methods"]
    coll.delete_many({})
    if results:
        coll.insert_many(results)
    print(f"✅ goal_methods: {len(results)} bodypart/situation combinations")
    return results


if __name__ == "__main__":
    total = raw.count_documents({})
    print(f"📊 Building insight collections from {total} raw events in '{config.RAW_EVENTS_COLLECTION}' ...")
    if total == 0:
        print("⚠️  No events found — run producer.py + consumer.py first.")
    rebuild_team_shot_efficiency()
    rebuild_home_away_comparison()
    rebuild_goal_methods()
    print("\n🎉 Done. New collections ready for Atlas Charts: "
          "team_shot_efficiency, home_away_comparison, goal_methods")
