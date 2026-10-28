-- Part 7: stage raw POS lines and one day of clickstream as Parquet on the host, for Spark to load (DuckDB).
-- Every event is kept, re-sends included: the Spark load and MERGE decide what "once" means.
COPY (
    SELECT b.basket_id,
           l.line_no,
           b.store_id,
           b.till_id,
           s.country,
           b.currency,
           b.event_ts,
           (b.event_ts AT TIME ZONE s.timezone)::DATE   AS business_date,
           l.sku,
           l.qty,
           l.unit_price_minor,
           l.discount_minor,
           l.qty * l.unit_price_minor - l.discount_minor AS amount_minor,
           b.uploaded_at,
           b.upload_date
    FROM raw_pos_events b,
         unnest(b.lines) AS t(l)
    JOIN raw_stores s ON s.store_id = b.store_id
) TO 'data/lab/part07/pos_lines' (FORMAT parquet, PARTITION_BY (upload_date), OVERWRITE true);

-- One day of app and web events. The fields that vary by event type travel as one JSON document, as the
-- default (format v2) tables store them; the v3 table parses the same text into a VARIANT.
COPY (
    SELECT event_id,
           event_type,
           event_ts,
           anonymous_id,
           platform,
           country,
           json_object('page', coalesce(page_path, page), 'sku', sku, 'order_id', order_id,
                       'app_version', app_version, 'user_agent', user_agent, 'consent', consent)::VARCHAR AS payload
    FROM raw_clickstream
    WHERE event_date = DATE '2026-09-15'
    QUALIFY row_number() OVER (PARTITION BY event_id ORDER BY event_ts) = 1   -- SDK retries (Part 6)
) TO 'data/lab/part07/clicks_2026-09-15.parquet' (FORMAT parquet);
