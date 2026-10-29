-- Part 20: the audit, run by spark/jobs/part20_wap.py against the audit branch before anything reaches main (Spark SQL).
-- Each statement returns (check, bad); bad must be 0. {audit} and {main} are the two refs of one table, {day} the
-- upload day being published, {raw} the landed POS events.

-- uniqueness: a basket line is in the fact once (a till's re-send must not become a second sale)
SELECT 'unique_line' AS check, count(*) AS bad
FROM (SELECT transaction_id, line_no FROM {audit} GROUP BY 1, 2 HAVING count(*) > 1);

-- completeness: every basket the tills uploaded on {day} is in the fact (loaded today or, if re-sent, earlier)
SELECT 'raw_basket_missing' AS check, count(*) AS bad
FROM (SELECT DISTINCT basket_id, event_ts FROM {raw} WHERE upload_date = DATE '{day}') r
LEFT ANTI JOIN {audit} f ON f.transaction_id = r.basket_id AND f.event_ts = r.event_ts;

-- consistency: what main already published for earlier upload days is untouched (rows and money)
SELECT 'history_changed' AS check,
       abs(a.n - m.n) + abs(a.minor - m.minor) AS bad
FROM (SELECT count(*) AS n, coalesce(sum(amount_minor), 0) AS minor FROM {audit} WHERE upload_date < DATE '{day}') a
CROSS JOIN (SELECT count(*) AS n, coalesce(sum(amount_minor), 0) AS minor FROM {main} WHERE upload_date < DATE '{day}') m;

-- validity: a sale line sells at least one unit, for no less than nothing
SELECT 'invalid_line' AS check, count(*) AS bad
FROM {audit} WHERE upload_date = DATE '{day}' AND (qty <= 0 OR amount_minor < 0);
