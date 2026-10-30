-- Part 32: retrieval over the rag database. `scripts.part32_models` runs each block by name with psycopg parameters:
-- %(q)s is the query vector ('[0.01,…]'), %(text)s the query words, %(country)s a country, %(n)s a candidate count.

-- name: vector
-- nearest chunks by cosine distance, through the HNSW index
SELECT doc_id, chunk_no, embedding <=> %(q)s::vector AS distance
FROM chunks
ORDER BY embedding <=> %(q)s::vector
LIMIT %(n)s;

-- name: vector_in_country
-- the same, for one country's agents: pgvector applies the filter after the index scan
SELECT doc_id, chunk_no, embedding <=> %(q)s::vector AS distance
FROM chunks
WHERE country = %(country)s
ORDER BY embedding <=> %(q)s::vector
LIMIT %(n)s;

-- name: keyword
-- full-text search, any of the query's words (plainto_tsquery ANDs them; '|' makes it OR)
SELECT doc_id, chunk_no, ts_rank_cd(tsv, query) AS score
FROM chunks, CAST(replace(plainto_tsquery('english', %(text)s)::text, '&', '|') AS tsquery) AS query
WHERE tsv @@ query
ORDER BY score DESC, doc_id, chunk_no
LIMIT %(n)s;

-- name: hybrid
-- reciprocal rank fusion of both lists, per document: score = sum of 1 / (60 + rank)
WITH by_vector AS (
    SELECT doc_id, rank() OVER (ORDER BY distance) AS r
    FROM (SELECT doc_id, embedding <=> %(q)s::vector AS distance FROM chunks
          ORDER BY embedding <=> %(q)s::vector LIMIT %(n)s) v
), by_words AS (
    SELECT doc_id, rank() OVER (ORDER BY score DESC) AS r
    FROM (SELECT doc_id, ts_rank_cd(tsv, query) AS score
          FROM chunks, CAST(replace(plainto_tsquery('english', %(text)s)::text, '&', '|') AS tsquery) AS query
          WHERE tsv @@ query ORDER BY score DESC LIMIT %(n)s) w
)
SELECT doc_id, sum(1.0 / (60 + r)) AS score
FROM (SELECT doc_id, min(r) AS r FROM by_vector GROUP BY doc_id
      UNION ALL
      SELECT doc_id, min(r) FROM by_words GROUP BY doc_id) ranked
GROUP BY doc_id
ORDER BY score DESC, doc_id
LIMIT %(n)s;
