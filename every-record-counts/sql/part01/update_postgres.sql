-- A missed £1 discount on one basket line, corrected in PostgreSQL.
-- ctid is the row version's physical address; xmin is the transaction that wrote it.
SELECT ctid, xmin, amount_minor, discount_minor
FROM sales_line WHERE basket_id = 'GB-0042-20260815-00009' AND line_no = 1;

UPDATE sales_line SET amount_minor = amount_minor - 100, discount_minor = discount_minor + 100
WHERE basket_id = 'GB-0042-20260815-00009' AND line_no = 1;

SELECT ctid, xmin, amount_minor, discount_minor
FROM sales_line WHERE basket_id = 'GB-0042-20260815-00009' AND line_no = 1;
