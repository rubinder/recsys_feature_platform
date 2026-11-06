#!/usr/bin/env bash
# Live (free-running) replay: 3 days of event time at 12h/s with injected malformed
# events, while watching three active users' features change in Redis.
set -euo pipefail
USERS=$(uv run python - <<'PY'
from recsys.streaming.events import ordered_events, history_end_seq
ev = ordered_events(); h = history_end_seq(ev)
w = ev.iloc[h:]; w = w[w.timestamp < w.timestamp.iloc[0] + __import__("pandas").Timedelta(days=3)]
print(",".join(str(u) for u in w.user_id.value_counts().index[:3]))
PY
)
echo "watching users $USERS"
uv run python -m recsys.streaming.watch --users "$USERS" --seconds 12 &
WATCH=$!
sleep 2
uv run python -m recsys.streaming.producer --days 3 --speedup 43200 --malformed 6
wait $WATCH
uv run python - <<'PY'
from recsys.streaming import kafka_io
print(f"dead-letter topic: {kafka_io.topic_size(kafka_io.DLQ_TOPIC)} malformed events rejected by the contract")
PY
