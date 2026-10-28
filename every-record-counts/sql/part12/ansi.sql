-- Part 12: ANSI mode, on by default since Spark 4.0 (Spark SQL). A bad value or an overflow fails the query
-- instead of quietly becoming NULL or wrapping around; try_* functions are the explicit opt-out per expression.
SET spark.sql.ansi.enabled;
SELECT CAST('12.40 GBP' AS BIGINT) AS amount_minor;
SELECT try_cast('12.40 GBP' AS BIGINT) AS amount_minor;
SELECT CAST(9223372036854775807 AS BIGINT) + 1 AS total_minor;
SELECT try_add(CAST(9223372036854775807 AS BIGINT), 1) AS total_minor;
SET spark.sql.ansi.enabled = false;
SELECT CAST('12.40 GBP' AS BIGINT) AS amount_minor, CAST(9223372036854775807 AS BIGINT) + 1 AS total_minor;
