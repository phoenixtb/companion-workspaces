# Part 31: Apache Superset with the one database driver it needs. The image ships without drivers; psycopg2 serves
# both Superset's own metadata database (the lab PostgreSQL) and Cube's SQL API, which speaks the PostgreSQL protocol.
# The pin is the one Superset's own `postgres` extra names (pyproject.toml in the image).
ARG SUPERSET_IMAGE
FROM ${SUPERSET_IMAGE}
USER root
RUN uv pip install --no-cache --python /app/.venv/bin/python psycopg2-binary==2.9.9
COPY superset_config.py /app/pythonpath/superset_config.py
USER superset
