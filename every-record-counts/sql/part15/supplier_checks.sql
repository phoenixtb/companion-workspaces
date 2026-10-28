-- Part 15: classify every landed supplier row (DuckDB). Nothing is dropped: each row is valid, a repeat copy of an
-- earlier row, or quarantined with a reason. raw_supplier_deliveries is the Iceberg raw table, every value text.

-- Parse with each supplier's declared date format; never guess DD/MM from the data.
CREATE OR REPLACE TABLE delivery_parsed AS
SELECT r.*,
       nullif(trim(r.po_number), '')                                                   AS po,
       TRY_CAST(r.po_line AS INTEGER)                                                  AS line_no,
       TRY_CAST(r.delivered_qty AS INTEGER)                                            AS qty,
       CASE coalesce(f.date_format, getvariable('default_date_format'))  -- strptime wants a constant format
            WHEN '%Y-%m-%d' THEN TRY_STRPTIME(r.delivery_date, '%Y-%m-%d')
            WHEN '%d/%m/%Y' THEN TRY_STRPTIME(r.delivery_date, '%d/%m/%Y')
       END::DATE                                                                       AS delivered_on
FROM raw_supplier_deliveries r
LEFT JOIN declared_date_format f USING (supplier_id);

-- A repeat is the same row, as sent, again: same supplier and values, in the same file or a later one.
CREATE OR REPLACE TABLE delivery_classified AS
SELECT *,
       CASE WHEN po IS NULL           THEN 'missing PO number'
            WHEN line_no IS NULL      THEN 'PO line not a number'
            WHEN qty IS NULL          THEN 'quantity not a number'
            WHEN qty < 0              THEN 'negative quantity'
            WHEN delivered_on IS NULL THEN 'date not valid in the declared format'
       END                                                                             AS issue,
       row_number() OVER sent_as                                                       AS copy_no,
       first_value(_source_file) OVER sent_as                                          AS first_file
FROM delivery_parsed
WINDOW sent_as AS (PARTITION BY supplier_id, po_number, po_line, sku, delivered_qty, delivery_date, dc
                   ORDER BY file_date, _source_file, _row_number);

CREATE OR REPLACE TABLE delivery_valid AS
SELECT supplier_id, po AS po_number, line_no AS po_line, sku, qty AS delivered_qty, delivered_on AS delivery_date,
       dc AS dc_id, file_date, _source_file AS source_file, _row_number AS source_row, _dlt_load_id AS load_id
FROM delivery_classified
WHERE issue IS NULL AND copy_no = 1;

CREATE OR REPLACE TABLE delivery_quarantine AS
SELECT supplier_id, issue, copy_no, po_number, po_line, sku, delivered_qty, delivery_date, dc AS dc_id, file_date,
       _source_file AS source_file, _row_number AS source_row, _dlt_load_id AS load_id
FROM delivery_classified
WHERE issue IS NOT NULL;
