-- Part 28: the two ways to keep the first copy of a basket, counted over the same replay.
-- Run with: python -m scripts.part28_flink dedup. Each SELECT prints its running count; the last row is the answer.
SET 'execution.runtime-mode' = 'streaming';

-- First copy by arrival (the view the job uses).
SELECT COUNT(*) AS baskets FROM baskets;

-- First copy by event time: emitted when the watermark passes the basket, so a basket behind the watermark is dropped.
SELECT COUNT(*) AS baskets FROM (
  SELECT *, ROW_NUMBER() OVER (PARTITION BY basket_id ORDER BY event_ts ASC) AS rn FROM pos_basket_events)
WHERE rn = 1;
