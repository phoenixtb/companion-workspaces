# Part 31: lab configuration, read from /app/pythonpath (the image puts it on PYTHONPATH). Development only.
import os

SECRET_KEY = os.environ["SUPERSET_SECRET_KEY"]
SQLALCHEMY_DATABASE_URI = f"postgresql+psycopg2://superset:{os.environ['SUPERSET_DB_PASSWORD']}@postgres:5432/superset"
# No Redis in the lab: CACHE_CONFIG and DATA_CACHE_CONFIG keep their default, NullCache, so Superset caches no query
# results. Cube does, one layer down, for every client at once.
