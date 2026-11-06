# 0002: Generic event / item / user schema

- Status: accepted
- Phase: 1

## Context
MovieLens 1M is the development dataset, but the project should be able to swap
in a music listening dataset (plays, skips, listen fractions) without touching
the feature pipeline.

## Options considered
1. Keep MovieLens's native columns (`rating`, `movie_id`, `genres` string).
2. A generic schema with a typed event weight and item attributes.

## Decision
Normalize every dataset at ingest into three Arrow-typed tables (`schema.py`):
- `events(user_id, item_id, event_type, weight, timestamp)`: MovieLens maps to
  `event_type="rate"`, `weight=rating`; music would be `"play"`, weight = play
  count or listen fraction.
- `items(item_id, title, genres)`: genres/tags as a list attribute.
- `users(user_id, attributes)`: free-form map.
Every downstream step reads only these tables. `schema.validate` casts to the
contract and fails on missing columns.

## Consequences
- Features are phrased generically (`user_avg_weight`, not `avg_rating`).
- Swapping datasets means writing one new ingest module.
- Only the `rate` event type exists today; multiple event types (e.g. play vs
  skip) would need per-type features, which the schema already allows.
