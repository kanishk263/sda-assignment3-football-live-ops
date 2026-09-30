# legend.py
# Decodes the numeric codes used in the Kaggle "football-events" dataset
# (secareanualin) into human-readable labels. Confirmed against real
# sample rows from the dataset.

EVENT_TYPE = {
    0: "Announcement", 1: "Attempt", 2: "Corner", 3: "Foul",
    4: "Yellow card", 5: "Second yellow card", 6: "Red card",
    7: "Substitution", 8: "Free kick won", 9: "Offside",
    10: "Hand ball", 11: "Penalty conceded",
}

EVENT_TYPE2 = {
    12: "Key Pass", 13: "Failed through ball",
    14: "Sending off", 15: "Own goal",
}

SIDE = {1: "Home", 2: "Away"}

SHOT_PLACE = {
    1: "Bit too high", 2: "Blocked", 3: "Bottom left corner",
    4: "Bottom right corner", 5: "Centre of the goal", 6: "High and wide",
    7: "Hits the bar", 8: "Misses to the left", 9: "Misses to the right",
    10: "Too high", 11: "Top centre of the goal", 12: "Top left corner",
    13: "Top right corner",
}

SHOT_OUTCOME = {1: "On target", 2: "Off target", 3: "Blocked", 4: "Hit the bar"}

LOCATION = {
    1: "Attacking half", 2: "Defensive half", 3: "Centre of the box",
    4: "Left wing", 5: "Right wing", 6: "Difficult angle & long range",
    7: "Difficult angle on the left", 8: "Difficult angle on the right",
    9: "Left side of the box", 10: "Left side of the six yard box",
    11: "Right side of the box", 12: "Right side of the six yard box",
    13: "Very close range", 14: "Penalty spot", 15: "Outside the box",
    16: "Long range", 17: "More than 35 yards", 18: "More than 40 yards",
    19: "Not recorded",
}

BODYPART = {1: "right foot", 2: "left foot", 3: "head"}

ASSIST_METHOD = {0: "None", 1: "Pass", 2: "Cross", 3: "Headed pass", 4: "Through ball"}

SITUATION = {1: "Open play", 2: "Set piece", 3: "Corner", 4: "Free kick"}


def _lookup(table, raw, default="Unknown"):
    if raw is None or raw == "":
        return None
    try:
        return table.get(int(float(raw)), default)
    except (ValueError, TypeError):
        return default


def decode_event(row):
    """Take a raw events.csv row (dict) and return a dict of decoded labels
    merged with the useful raw fields. Numeric/blank fields are left as
    None when not applicable to this event type (e.g. shot_place on a
    Foul event)."""

    et_raw = row.get("event_type")
    et2_raw = row.get("event_type2")

    event_type_label = _lookup(EVENT_TYPE, et_raw)
    event_type2_label = _lookup(EVENT_TYPE2, et2_raw) if et2_raw not in (None, "", "NA") else None

    is_goal = str(row.get("is_goal", "0")).strip() in ("1", "1.0", "True", "true")

    return {
        "id_odsp": row.get("id_odsp"),
        "id_event": row.get("id_event"),
        "sort_order": int(float(row.get("sort_order", 0) or 0)),
        "minute": int(float(row.get("time", 0) or 0)),
        "text": row.get("text"),
        "event_type": event_type_label,
        "event_type2": event_type2_label,
        "side": _lookup(SIDE, row.get("side")),
        "event_team": row.get("event_team"),
        "opponent": row.get("opponent"),
        "player": row.get("player") or None,
        "player2": row.get("player2") or None,
        "player_in": row.get("player_in") or None,
        "player_out": row.get("player_out") or None,
        "shot_place": _lookup(SHOT_PLACE, row.get("shot_place")) if row.get("shot_place") not in (None, "", "NA") else None,
        "shot_outcome": _lookup(SHOT_OUTCOME, row.get("shot_outcome")) if row.get("shot_outcome") not in (None, "", "NA") else None,
        "is_goal": is_goal,
        "location": _lookup(LOCATION, row.get("location")) if row.get("location") not in (None, "", "NA") else None,
        "bodypart": _lookup(BODYPART, row.get("bodypart")) if row.get("bodypart") not in (None, "", "NA") else None,
        "assist_method": _lookup(ASSIST_METHOD, row.get("assist_method")) if row.get("assist_method") not in (None, "", "NA") else None,
        "situation": _lookup(SITUATION, row.get("situation")) if row.get("situation") not in (None, "", "NA") else None,
    }
