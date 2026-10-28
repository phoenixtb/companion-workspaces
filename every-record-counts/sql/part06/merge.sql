-- Part 6: MERGE (upsert) semantics in DuckDB. A daily store-sales table loaded from till uploads up to 14 September,
-- then the 15 September upload arrives, carrying late baskets for days already loaded.
CREATE OR REPLACE TEMP MACRO store_days(until_upload) AS TABLE
    SELECT e.store_id, (e.event_ts AT TIME ZONE s.timezone)::DATE AS business_date,
           count(*) AS baskets, sum(e.tenders[1].amount_minor) AS sales_minor
    FROM (SELECT * FROM raw_pos_events WHERE upload_date <= until_upload
          QUALIFY row_number() OVER (PARTITION BY basket_id ORDER BY uploaded_at, event_id) = 1) e
    JOIN raw_stores s USING (store_id)
    GROUP BY ALL;

-- The (store, day) pairs one upload touched, recomputed from everything landed up to and including it.
CREATE OR REPLACE TEMP MACRO touched(upload) AS TABLE
    SELECT d.* FROM store_days(upload) d
    SEMI JOIN (SELECT e.store_id, (e.event_ts AT TIME ZONE s.timezone)::DATE AS business_date
               FROM raw_pos_events e JOIN raw_stores s USING (store_id) WHERE e.upload_date = upload) t
         USING (store_id, business_date);

-- What the new upload adds, counted on its own (the "just add the increment" approach).
CREATE OR REPLACE TEMP MACRO increment(upload) AS TABLE
    SELECT e.store_id, (e.event_ts AT TIME ZONE s.timezone)::DATE AS business_date,
           count(*) AS baskets, sum(e.tenders[1].amount_minor) AS sales_minor
    FROM (SELECT * FROM raw_pos_events WHERE upload_date = upload
          QUALIFY row_number() OVER (PARTITION BY basket_id ORDER BY uploaded_at, event_id) = 1) e
    JOIN raw_stores s USING (store_id)
    GROUP BY ALL;

CREATE OR REPLACE TABLE merge_log (step VARCHAR, store_days BIGINT, baskets HUGEINT, sales_minor HUGEINT);
CREATE OR REPLACE TABLE daily_store_sales AS FROM store_days(DATE '2026-09-14');
CREATE OR REPLACE TABLE daily_store_sales_inc AS FROM daily_store_sales;
INSERT INTO merge_log SELECT '0. loaded up to 14 Sep', count(*), sum(baskets), sum(sales_minor) FROM daily_store_sales;

-- Upsert recomputed rows: update a (store, day) if its numbers changed, insert it if it's new.
MERGE INTO daily_store_sales t
USING touched(DATE '2026-09-15') s
ON t.store_id = s.store_id AND t.business_date = s.business_date
WHEN MATCHED AND (t.baskets, t.sales_minor) IS DISTINCT FROM (s.baskets, s.sales_minor)
    THEN UPDATE SET baskets = s.baskets, sales_minor = s.sales_minor
WHEN NOT MATCHED THEN INSERT BY NAME;
INSERT INTO merge_log SELECT '1. recomputed MERGE, run 1', count(*), sum(baskets), sum(sales_minor) FROM daily_store_sales;

MERGE INTO daily_store_sales t
USING touched(DATE '2026-09-15') s
ON t.store_id = s.store_id AND t.business_date = s.business_date
WHEN MATCHED AND (t.baskets, t.sales_minor) IS DISTINCT FROM (s.baskets, s.sales_minor)
    THEN UPDATE SET baskets = s.baskets, sales_minor = s.sales_minor
WHEN NOT MATCHED THEN INSERT BY NAME;
INSERT INTO merge_log SELECT '2. recomputed MERGE, run 2', count(*), sum(baskets), sum(sales_minor) FROM daily_store_sales;

-- Upsert increments: add the new upload's numbers to what's there. Run it twice, as a retry would.
MERGE INTO daily_store_sales_inc t
USING increment(DATE '2026-09-15') s
ON t.store_id = s.store_id AND t.business_date = s.business_date
WHEN MATCHED THEN UPDATE SET baskets = t.baskets + s.baskets, sales_minor = t.sales_minor + s.sales_minor
WHEN NOT MATCHED THEN INSERT BY NAME;
INSERT INTO merge_log SELECT '3. increment MERGE, run 1', count(*), sum(baskets), sum(sales_minor) FROM daily_store_sales_inc;

MERGE INTO daily_store_sales_inc t
USING increment(DATE '2026-09-15') s
ON t.store_id = s.store_id AND t.business_date = s.business_date
WHEN MATCHED THEN UPDATE SET baskets = t.baskets + s.baskets, sales_minor = t.sales_minor + s.sales_minor
WHEN NOT MATCHED THEN INSERT BY NAME;
INSERT INTO merge_log SELECT '4. increment MERGE, run 2', count(*), sum(baskets), sum(sales_minor) FROM daily_store_sales_inc;

INSERT INTO merge_log SELECT '5. truth: rebuilt from all uploads to 15 Sep', count(*), sum(baskets), sum(sales_minor)
FROM store_days(DATE '2026-09-15');

SELECT * FROM merge_log ORDER BY step;

-- Why one increment run is already off: re-sends in the 15 September upload whose original came in an earlier one.
SELECT count(*) AS resends_of_earlier_uploads
FROM (SELECT basket_id FROM raw_pos_events WHERE upload_date = DATE '2026-09-15') n
SEMI JOIN (SELECT basket_id FROM raw_pos_events WHERE upload_date < DATE '2026-09-15') o USING (basket_id);

-- A source with duplicate keys. Matched duplicates are an error; unmatched duplicates are inserted twice.
CREATE OR REPLACE TABLE basket_totals AS
SELECT basket_id, tenders[1].amount_minor AS amount_minor FROM raw_pos_events WHERE upload_date < DATE '2026-09-15'
QUALIFY row_number() OVER (PARTITION BY basket_id ORDER BY uploaded_at, event_id) = 1;

MERGE INTO basket_totals t
USING (SELECT basket_id, tenders[1].amount_minor AS amount_minor FROM raw_pos_events
       WHERE basket_id IN (SELECT basket_id FROM raw_pos_events GROUP BY ALL HAVING count(*) > 1)) s
ON t.basket_id = s.basket_id
WHEN MATCHED THEN UPDATE SET amount_minor = s.amount_minor
WHEN NOT MATCHED THEN INSERT BY NAME;

MERGE INTO basket_totals t
USING (SELECT basket_id, tenders[1].amount_minor AS amount_minor FROM raw_pos_events
       WHERE basket_id IN (SELECT basket_id FROM raw_pos_events GROUP BY ALL HAVING count(*) > 1)) s
ON t.basket_id = s.basket_id
WHEN NOT MATCHED THEN INSERT BY NAME;

SELECT count(*) AS rows, count(DISTINCT basket_id) AS baskets FROM basket_totals;
