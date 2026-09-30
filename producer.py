"""
producer.py
Assignment 3 — Live Match-Day Ops Dashboard (Football)
Streaming Data Analytics | MBA Course

Reads REAL historical match data (Kaggle "football-events" dataset by
secareanualin — events.csv + ginf.csv, ~900,000 rows), filters it down to
one real matchday, and replays every event across all matches of that
day as a single interleaved live Kafka stream — exactly how a broadcaster's
live-ops monitor watches several simultaneous kickoffs at once.

This is "feed replay": treating real historical event data as a live feed
by streaming it in chronological order with a pacing delay. It is the same
technique sports-data vendors (Opta, StatsBomb, Sportradar) use to test and
demo live systems before/between live windows. Every event streamed here
really happened — nothing about the match content is invented.

Usage:
    python producer.py                       # browse ginf.csv and pick a matchday interactively
    python producer.py --date 06/08/11        # skip browsing, go straight to a known date
    python producer.py --events /path/to/events.csv --ginf /path/to/ginf.csv
    python producer.py --limit 500            # quick smoke test
    python producer.py --speed 0               # no delay, fire as fast as possible

If --events/--ginf are omitted, config.EVENTS_CSV / config.GINF_CSV are
used. The bundled sample_data/ folder has a small synthetic file with the
exact same columns/legend so this script can be smoke-tested without the
real 900k-row files.

Nothing about a specific matchday is hardcoded — the browser below reads
whatever dates actually exist in ginf.csv, so it works for any date range
the real 900k-row dataset covers, not just the one matchday used to build
and test this pipeline.
"""

import argparse
import csv
import json
import sys
import time
from collections import defaultdict
from datetime import datetime, timezone

from kafka import KafkaProducer, errors

import config
from legend import decode_event

CARD_EMOJI = {"Yellow card": "🟨", "Second yellow card": "🟨🟨", "Red card": "🟥"}


def now_iso():
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def _date_sort_key(date_str):
    try:
        return datetime.strptime(date_str, "%d/%m/%y")
    except ValueError:
        return datetime.min


def scan_available_dates(ginf_path):
    """One pass over ginf.csv: for every date that has at least one match,
    collect how many matches, and which leagues/countries played that day.
    Returns a list of dicts, most recent date first."""
    info = defaultdict(lambda: {"count": 0, "leagues": set(), "countries": set()})
    with open(ginf_path, newline="", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        for row in reader:
            d = (row.get("date") or "").strip()
            if not d:
                continue
            info[d]["count"] += 1
            if row.get("league"):
                info[d]["leagues"].add(row["league"])
            if row.get("country"):
                info[d]["countries"].add(row["country"])

    dates = [
        {"date": d, "count": v["count"],
         "leagues": sorted(v["leagues"]), "countries": sorted(v["countries"])}
        for d, v in info.items()
    ]
    dates.sort(key=lambda x: _date_sort_key(x["date"]), reverse=True)
    return dates


def browse_and_select_date(ginf_path, page_size=15):
    """Interactive terminal browser: list matchdays found in ginf.csv,
    optionally narrowed by league, and let the user pick one by number.
    Returns the chosen date string (DD/MM/YY, matching ginf.csv exactly)."""
    print(f"🔎 Scanning {ginf_path} for available matchdays ...")
    all_dates = scan_available_dates(ginf_path)
    if not all_dates:
        print(f"❌ No dates found in {ginf_path}")
        sys.exit(1)
    print(f"📅 {len(all_dates)} distinct matchdays found.\n")

    dates = all_dates
    league_filter = None
    page = 0

    while True:
        start, end = page * page_size, page * page_size + page_size
        shown = dates[start:end]
        if not shown and page > 0:
            page = 0
            continue

        label = f" (league={league_filter})" if league_filter else ""
        print(f"--- Matchdays{label} — page {page + 1} of {max(1, -(-len(dates) // page_size))} ---")
        for i, d in enumerate(shown, start=1):
            leagues = ",".join(d["leagues"]) or "?"
            print(f"  {i:>2}) {d['date']}  —  {d['count']} match(es)  [{leagues}]")

        print("\nType a number to pick that date, 'n'/'p' for next/prev page,")
        print("'f <league_code>' to filter (e.g. 'f D1'), 'f' to clear filter, or 'q' to quit.")
        choice = input("> ").strip()

        if choice.lower() == "q":
            print("Cancelled.")
            sys.exit(0)
        elif choice.lower() == "n":
            page += 1
        elif choice.lower() == "p":
            page = max(0, page - 1)
        elif choice.lower().startswith("f"):
            arg = choice[1:].strip()
            if arg:
                league_filter = arg.upper()
                dates = [d for d in all_dates if league_filter in d["leagues"]]
            else:
                league_filter = None
                dates = all_dates
            page = 0
        elif choice.isdigit() and 1 <= int(choice) <= len(shown):
            return shown[int(choice) - 1]["date"]
        else:
            print("Didn't catch that — try again.\n")


def parse_ginf(ginf_path, target_date):
    """Return {id_odsp: match_info_dict} for every match on target_date."""
    matches = {}
    with open(ginf_path, newline="", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        for row in reader:
            if row.get("date", "").strip() != target_date:
                continue
            matches[row["id_odsp"]] = {
                "id_odsp": row["id_odsp"],
                "date": row["date"],
                "league": row.get("league"),
                "country": row.get("country"),
                "season": row.get("season"),
                "home_team": row.get("ht"),
                "away_team": row.get("at"),
                "final_home_goals": row.get("fthg"),
                "final_away_goals": row.get("ftag"),
            }
    return matches


def load_events(events_path, match_ids):
    """Decode every events.csv row belonging to one of match_ids."""
    decoded = []
    with open(events_path, newline="", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        for row in reader:
            if row.get("id_odsp") not in match_ids:
                continue
            decoded.append(decode_event(row))
    return decoded


def build_stream(matches, events):
    """Attach match context to every event and sort them so all matches
    of the day are interleaved chronologically (as if kicking off at the
    same time) — minute first, then sort_order as a tiebreak within the
    same minute."""
    stream = []
    for e in events:
        m = matches[e["id_odsp"]]
        e["home_team"] = m["home_team"]
        e["away_team"] = m["away_team"]
        e["league"] = m["league"]
        e["country"] = m["country"]
        e["match_label"] = f"{m['home_team']} vs {m['away_team']}"
        stream.append(e)
    stream.sort(key=lambda e: (e["minute"], e["sort_order"]))
    return stream


def goal_scoring_team(event):
    """Whoever the goal is credited to. An own goal (event_type2 ==
    'Own goal') counts for the opponent, not event_team."""
    if event.get("event_type2") == "Own goal":
        return event["opponent"]
    return event["event_team"]


def format_console_line(event):
    et = event["event_type"]
    minute = event["minute"]
    label = event["match_label"]
    player = event.get("player") or event.get("event_team") or "?"

    if event.get("is_goal"):
        return f"⚽ [{minute}'] GOAL — {player} ({event['event_team']}) | {label}"
    if et in ("Yellow card", "Second yellow card", "Red card"):
        emoji = CARD_EMOJI.get(et, "🟨")
        return f"{emoji} [{minute}'] {et.upper()} — {player} ({event['event_team']}) | {label}"
    if et == "Substitution":
        return f"🔄 [{minute}'] SUB — {event.get('player_out') or '?'} → {event.get('player_in') or '?'} ({event['event_team']}) | {label}"
    if et == "Attempt":
        outcome = event.get("shot_outcome") or "?"
        return f"🎯 [{minute}'] SHOT ({outcome}) — {player} ({event['event_team']}) | {label}"
    if et == "Corner":
        return f"🚩 [{minute}'] CORNER — {event['event_team']} | {label}"
    if et == "Foul":
        return f"⚠️  [{minute}'] FOUL — {player} ({event['event_team']}) | {label}"
    return f"⚪ [{minute}'] {et} — {event['event_team']} | {label}"


def connect_producer():
    try:
        producer = KafkaProducer(
            bootstrap_servers=config.KAFKA_BROKER,
            value_serializer=lambda v: json.dumps(v).encode("utf-8"),
            retries=5,
        )
        producer.partitions_for(config.TOPIC)
        print(f"✅ Connected to Kafka broker at {config.KAFKA_BROKER}")
        return producer
    except errors.NoBrokersAvailable:
        print(f"❌ Kafka broker not available at {config.KAFKA_BROKER}")
        sys.exit(1)
    except Exception as e:
        print("❌ Error connecting to Kafka:", str(e))
        sys.exit(1)


def main():
    parser = argparse.ArgumentParser(description="Replay a real football matchday as a live Kafka stream")
    parser.add_argument("--events", default=config.EVENTS_CSV, help="path to events.csv")
    parser.add_argument("--ginf", default=config.GINF_CSV, help="path to ginf.csv")
    parser.add_argument("--date", default=None,
                         help="matchday date, DD/MM/YY, matching ginf.csv. Omit to browse and pick interactively.")
    parser.add_argument("--speed", type=float, default=config.STREAM_DELAY_SECONDS, help="seconds between events")
    parser.add_argument("--limit", type=int, default=None, help="stream only the first N events (smoke test)")
    args = parser.parse_args()

    if args.date is None:
        args.date = browse_and_select_date(args.ginf)

    print(f"\n📄 Loading matches on {args.date} from {args.ginf} ...")
    matches = parse_ginf(args.ginf, args.date)
    if not matches:
        print(f"❌ No matches found for date {args.date} in {args.ginf}")
        sys.exit(1)

    print(f"🏟  Found {len(matches)} matches:")
    for m in matches.values():
        print(f"    {m['home_team']} vs {m['away_team']}  ({m['league']}, {m['country']})")

    print(f"📄 Loading events from {args.events} ...")
    events = load_events(args.events, set(matches.keys()))
    print(f"📦 {len(events)} raw events loaded across all {len(matches)} matches")

    stream = build_stream(matches, events)
    if args.limit:
        stream = stream[: args.limit]

    if sys.stdin.isatty():
        confirm = input(f"\nStream these {len(matches)} match(es) now? [Y/n] ").strip().lower()
        if confirm in ("n", "no"):
            print("Cancelled.")
            sys.exit(0)

    producer = connect_producer()
    print(f"📡 Streaming {len(stream)} events → topic '{config.TOPIC}' "
          f"({args.speed}s between events). Press Ctrl+C to stop.\n")

    score = {mid: {"home": 0, "away": 0} for mid in matches}

    try:
        for event in stream:
            m = matches[event["id_odsp"]]

            if event.get("is_goal"):
                scoring_team = goal_scoring_team(event)
                if scoring_team == m["home_team"]:
                    score[event["id_odsp"]]["home"] += 1
                else:
                    score[event["id_odsp"]]["away"] += 1

            payload = {
                **event,
                "match_id": event["id_odsp"],
                "matchday_date": args.date,
                "timestamp": now_iso(),
                "score_home": score[event["id_odsp"]]["home"],
                "score_away": score[event["id_odsp"]]["away"],
            }
            producer.send(config.TOPIC, value=payload)
            print(format_console_line(event))

            if args.speed > 0:
                time.sleep(args.speed)

        print("\n🏁 MATCHDAY COMPLETE — final scores:")
        for mid, m in matches.items():
            s = score[mid]
            print(f"    {m['home_team']} {s['home']} - {s['away']} {m['away_team']}")

        producer.send(config.TOPIC, value={
            "event_type": "MATCHDAY_COMPLETE",
            "timestamp": now_iso(),
            "matchday_date": args.date,
            "date": args.date,
            "matches": [
                {"match_id": mid, "matchday_date": args.date,
                 "home_team": m["home_team"], "away_team": m["away_team"],
                 "score_home": score[mid]["home"], "score_away": score[mid]["away"]}
                for mid, m in matches.items()
            ],
        })

    except KeyboardInterrupt:
        print("\n🛑 Stopped streaming.")
    finally:
        producer.flush()
        producer.close()


if __name__ == "__main__":
    main()
