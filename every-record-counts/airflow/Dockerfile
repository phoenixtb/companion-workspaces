# Part 19: Airflow 3.3 plus the lab's own Python environment, side by side.
# Airflow keeps its interpreter and its constraint-pinned packages; the lab's code (dlt, PyIceberg, DuckDB, dbt-core +
# dbt-trino) goes into a separate virtualenv built from the lab's uv.lock, so neither can break the other's pins.
# Tasks call /opt/lab-venv/bin/python and /opt/lab-venv/bin/dbt; the lab folder itself is mounted at /opt/lab.
ARG AIRFLOW_IMAGE
ARG UV_IMAGE
FROM ${UV_IMAGE} AS uv

FROM ${AIRFLOW_IMAGE}
COPY --from=uv /uv /usr/local/bin/uv
ENV UV_PROJECT_ENVIRONMENT=/opt/lab-venv \
    UV_PYTHON_INSTALL_DIR=/opt/uv-python \
    UV_LINK_MODE=copy \
    UV_COMPILE_BYTECODE=1
# Part 22: the image ships apache-airflow-providers-openlineage 2.20.1; pin the current release into Airflow's own
# interpreter (Airflow itself pinned, so pip can't move it).
RUN pip install --no-cache-dir "apache-airflow==3.3.2" "apache-airflow-providers-openlineage==2.20.3"
USER root
RUN mkdir -p /opt/lab-venv /opt/uv-python /opt/lab-build && chown airflow:0 /opt/lab-venv /opt/uv-python /opt/lab-build
USER airflow
WORKDIR /opt/lab-build
COPY --chown=airflow:0 pyproject.toml uv.lock .python-version ./
# The lab pins Python 3.13 (.python-version); uv downloads it, Airflow's own interpreter stays 3.12.
RUN uv sync --frozen --no-dev --extra dbt --no-install-project && uv cache clean
WORKDIR /opt/airflow
