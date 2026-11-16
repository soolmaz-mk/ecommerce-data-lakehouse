# Airflow image for orchestration + a dedicated, isolated Silver venv.
#
# The Silver layer runs on Polars + PyIceberg (design doc PART 6), which need
# Python >=3.10 and their own pyarrow -- incompatible with both the Spark image
# (Python 3.8) and Airflow's pinned constraints. So instead of baking Spark/JVM
# into Airflow (heavy, and unnecessary once Silver leaves Spark), we build a
# separate venv at /opt/venvs/silver and run each Silver task in it via
# ExternalPythonOperator. No JVM, no spark-submit, no docker socket.
ARG AIRFLOW_BASE_IMAGE=apache/airflow:3.2.1
FROM ${AIRFLOW_BASE_IMAGE}

# Build the isolated Silver venv. The base image's default user is `airflow`,
# which cannot write to root-owned /opt -- so create the venv as root, then hand
# it to the airflow user and switch back. --system-site-packages is deliberately
# NOT set: the venv's dependency tree stays fully separate from Airflow's.
USER root
COPY jobs/silver/requirements.txt /tmp/silver-requirements.txt
RUN python -m venv /opt/venvs/silver && \
    /opt/venvs/silver/bin/pip install --no-cache-dir --upgrade pip && \
    /opt/venvs/silver/bin/pip install --no-cache-dir -r /tmp/silver-requirements.txt && \
    /opt/venvs/silver/bin/python -c "import polars, pyiceberg, pyarrow; print('silver venv ok', polars.__version__, pyiceberg.__version__)" && \
    chown -R airflow:0 /opt/venvs

USER airflow
