# MongoDB Atlas Charts — Setup Guide

Builds ≥3 charts (4 provided) on top of the aggregate collections written by
`consumer.py`, using the same Atlas cluster (`clusterkanishk`) already set up
for the Lecture 10 fraud pipeline, in the new `football_live_ops` database.

## 1. Open Charts

1. Log in to MongoDB Atlas → open your project.
2. Left sidebar → **Charts**. If this is the first time, click **Activate
   Charts** (it attaches to your existing cluster, no new cluster needed).

## 2. Connect the data source

1. Charts → **Data Sources** → **Add Data Source**.
2. Choose your cluster (`clusterkanishk`).
3. Select the **`football_live_ops`** database and enable these
   collections as data sources:
   - `goals_timeline`
   - `cards_by_team`
   - `shot_outcomes`
   - `live_scoreboard`
4. Run `producer.py` + `consumer.py` at least once first, so these
   collections actually have documents — Charts can't preview an empty
   collection's fields well.

## 3. Chart 1 — Cumulative Score Race (line chart, by minute)

A plain "goals per 5-minute bucket" bar chart is flat and doesn't tell a
story once you have more than a handful of matches. Use a **cumulative
line chart** instead — a small multiples "race" of every match's scoreline
building over the 90 minutes, which is exactly what a live-ops monitor is
actually for (which matches are heating up, which are goal-less).

- **Dashboard** → New Dashboard → "Live Match-Day Ops".
- **Add Chart** → Data source: `goals_timeline`.
- Chart type: **Line**.
- X Axis: `minute` (no binning — keep it continuous).
- Y Axis: `_id`, Aggregate = **Count**, then toggle **Accumulate** ON
  (this is what turns "goals at minute X" into "running total up to
  minute X" — the actual insight).
- Series: `match_label` (now genuinely useful — each line is one match's
  scoring race, not a cluttered legend, since it's separating *lines*, not
  splitting bars).
- Title: "Cumulative goals by match — matchday".

## 4. Chart 2 — Cards by Team (discipline)

- Add Chart → Data source: `cards_by_team`.
- Chart type: **Stacked bar chart**.
- X axis: `team`.
- Y axis: `tally.Yellow card`, `tally.Red card`, `tally.Second yellow card`
  (as separate series/stacked segments — Charts lets you add each `tally.*`
  field as its own series).
- Title: "Cards by team".
- Sort X axis descending by total for a quick "who's playing rough" read.

## 5. Chart 3 — Shot Outcomes

- Add Chart → Data source: `shot_outcomes`.
- Chart type: **Donut chart**.
- Category (angle): `outcome`.
- Value: `count`.
- Title: "Shot outcomes across all matches (On target / Off target / Blocked / Hit the bar)".

## 6. Chart 4 — Live Scoreboard

- Add Chart → Data source: `live_scoreboard`.
- Chart type: **Table** (or Grid).
- Columns: `home_team`, `score_home`, `score_away`, `away_team`,
  `last_minute`, `league`.
- Sort by `last_minute` descending.
- Title: "Live scoreboard — all matches".
- This is the single-glance control-room view: every match, current score,
  and how far in it is, updated continuously as `consumer.py` upserts it.

## 6b. Run `build_insights.py` and connect 3 more data sources

Charts 5-7 below need real ratios (conversion %, home-vs-away splits,
body-part × situation breakdowns) that plain field encoding in Charts
can't compute — Charts can show you a field's value, but it can't derive
"goals ÷ shots" on its own. So this comes from a script that runs proper
MongoDB aggregation pipelines over the raw `match_events` collection:

```
python build_insights.py
```

Run it any time after streaming at least one matchday (safe to re-run —
it fully rebuilds its 3 collections from the raw layer each time). Then
go back to **Charts → Data Sources → Add Data Source** and add:
- `team_shot_efficiency`
- `home_away_comparison`
- `goal_methods`

## 7. Chart 5 — Team Shot Efficiency (attacking quality, not just volume)

- Data source: `team_shot_efficiency`.
- Chart type: **Scatter**.
- X Axis: `shots_total` (how much a team shoots).
- Y Axis: `conversion_rate_pct` (how often those shots become goals).
- Color: `team`.
- Title: "Attacking efficiency: shot volume vs conversion rate".
- The insight: teams in the top-left (few shots, high conversion) are
  clinical finishers; teams bottom-right (lots of shots, low conversion)
  are wasteful in front of goal — a genuinely different read than "who
  shot the most."

## 8. Chart 6 — Home vs Away Comparison

- Data source: `home_away_comparison`.
- Chart type: **Grouped Column**.
- X Axis: pick the metric fields as separate Y-axis series instead —
  actually set X Axis = `side` (Home/Away), Y Axis = add `goals`, `cards`,
  `fouls`, `shots_on_target` as four separate Y-axis fields (grouped, not
  stacked).
- Title: "Home vs Away — goals, cards, fouls, shots on target".
- The insight: this is the closest thing to testing "home advantage" in
  the data — more attacking output and fewer cards for the home side is
  the classic pattern; see if this matchday actually shows it.

## 9. Chart 7 — How Goals Are Scored

- Data source: `goal_methods`.
- Chart type: **Stacked Column** (or Heatmap if your Charts version offers it).
- X Axis: `situation` (Open play / Set piece / Corner / Free kick).
- Y Axis: `count`, Aggregate = **Sum**.
- Series: `bodypart` (right foot / left foot / head).
- Title: "How goals are scored — body part × situation".
- The insight: shows whether goals cluster around a specific
  combination (e.g. headers from corners, or right-foot finishes in open
  play) — a genuine tactical read, not just a total.

## 10. (Optional Chart 8) Event Type Distribution

- Add Chart → Data source: `event_type_distribution`.
- Chart type: **Bar chart**, X axis `event_type`, Y axis `count`, sorted
  descending — shows the overall shape of the match day (mostly passes/
  fouls/attempts, few cards/goals — useful sanity check that the pipeline
  is capturing the full event mix, not just the highlights).

## 11. Filter by matchday (so multiple days don't blend together)

Every event, goal, card tally, shot tally and scoreboard entry carries a
**`matchday_date`** field (whatever date you picked when running
`producer.py`). If you run the pipeline for more than one date, Mongo
keeps every date's data — it doesn't overwrite the previous run — so add
a **Dashboard Filter**:

1. On the dashboard, click **Filters** → **Add Filter**.
2. Field: `matchday_date`. Type: dropdown/select.
3. This gives you one control at the top of the dashboard to switch which
   matchday all 4-5 charts are showing, instead of rerunning anything.

## 12. Auto-refresh for a "live" feel

- On the dashboard, use the **refresh interval** control (top right) and
  set it to 10–30 seconds. While `producer.py`/`consumer.py` are running,
  the dashboard will visibly update as new events land — this is what
  makes it a *live* ops dashboard rather than a static report.

## 13. Sharing / screenshotting for submission

- Dashboard → **Share** → get an embed/share link, or just screenshot each
  chart after letting the pipeline run for a minute or two so there's
  real data to show (goals, cards, and a few different scorelines).
