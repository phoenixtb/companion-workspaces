-- Part 32: exact nearest neighbours in DuckDB over the tagged lakehouse snapshot: no index, every vector compared.
SELECT doc_id, chunk_no, array_cosine_distance(embedding, ?::FLOAT[1024]) AS distance
FROM corpus
ORDER BY distance, doc_id, chunk_no
LIMIT 10;
