"""
Launcher: one spark-submit per Kafka topic (N topics -> N Spark apps), so a
schema change or failure on one topic only touches that job. Runs under plain
python3, NOT spark-submit - deliberately zero pyspark import here (only
main.run(), which spark-submit's child process loads, needs it).

Supervises the children and respawns each independently when it dies - one
topic crash-looping (e.g. Schema Registry unreachable) never touches the
others, which is the whole point of one-job-per-topic. The container's
restart policy is only the fallback for the launcher process itself dying.
"""

from __future__ import annotations

import os
import subprocess
import time

from .config import BronzeConfig
from .logger import get_logger

logger = get_logger(__name__)

# stagger submissions - don't storm Kafka / start N driver JVMs all at once
_SUBMIT_STAGGER_SECONDS = 3
# how often the launcher polls its child jobs
_SUPERVISE_POLL_SECONDS = 5
# crash-loop backoff for a topic's job dying: starts here, doubles each
# consecutive fast failure, capped at _MAX_RESPAWN_BACKOFF_SECONDS. A flat 10s
# forever is fine for a blip but ruinous for a permanently-broken topic (e.g.
# no Avro schema registered yet) - observed hammering the master every 10s
# for 9+ hours straight, competing for scheduling against topics that could
# actually make progress. A run judged "fast" (see _FAST_FAILURE_THRESHOLD_S)
# escalates the backoff; a run that stayed up a while resets it to the floor.
_RESPAWN_BACKOFF_SECONDS = 10
_MAX_RESPAWN_BACKOFF_SECONDS = 300
_FAST_FAILURE_THRESHOLD_SECONDS = 30

_SPARK_SUBMIT = os.environ.get("SPARK_SUBMIT_BIN", "/opt/spark/bin/spark-submit")
_SPARK_MASTER = os.environ.get("SPARK_MASTER_URL", "spark://spark-master:7077")
_CLUSTER_CORES = int(os.environ.get("SPARK_WORKER_CORES", "8"))
_MIN_EXECUTOR_MEMORY_MB = 512


def _parse_spark_memory_mb(value: str) -> int:
    """Parse a Spark-style JVM memory string (e.g. "2G", "512m") to MB."""
    value = value.strip().lower()
    units = {"g": 1024, "m": 1, "k": 1 / 1024, "t": 1024 * 1024}
    for suffix, factor in units.items():
        if value.endswith(suffix):
            return int(float(value[: -len(suffix)]) * factor)
    return int(value)  # bare number: Spark treats this as bytes


_CLUSTER_MEMORY_MB = _parse_spark_memory_mb(os.environ.get("SPARK_WORKER_MEMORY", "2g"))


def _cores_per_topic(topic_count: int) -> int:
    # split the cluster's cores evenly across topics, floor 1 so nothing is
    # ever granted zero (which is what caused the starvation in the first place)
    return max(1, _CLUSTER_CORES // max(topic_count, 1))


def _memory_mb_per_topic(topic_count: int) -> int:
    # same idea as cores, for memory - see _CLUSTER_CORES comment above
    return max(_MIN_EXECUTOR_MEMORY_MB, _CLUSTER_MEMORY_MB // max(topic_count, 1))


def _submit(app_path: str, topic: str, cores: int, memory_mb: int) -> subprocess.Popen:
    # each topic = its own spark-submit application (client mode); name it per
    # topic via SPARK_APP_NAME -> config.app_name -> SparkSession appName.
    env = {**os.environ, "SPARK_APP_NAME": f"bronze-ingestion-{topic}"}
    cmd = [
        _SPARK_SUBMIT,
        "--master", _SPARK_MASTER,
        "--driver-memory", "512m",
        "--total-executor-cores", str(cores),
        "--executor-memory", f"{memory_mb}m",
        app_path,
        "--topic", topic,
    ]
    logger.info(f"submitting bronze job for topic={topic} cores={cores} executor_memory={memory_mb}m")
    return subprocess.Popen(cmd, env=env)


def submit_all(app_path: str) -> None:
    config = BronzeConfig.from_env()
    topic_count = len(config.kafka_topics)
    cores = _cores_per_topic(topic_count)
    memory_mb = _memory_mb_per_topic(topic_count)
    procs: "dict[str, subprocess.Popen]" = {}
    spawned_at: "dict[str, float]" = {}
    consecutive_fast_failures: "dict[str, int]" = {}
    for index, topic in enumerate(config.kafka_topics):
        if index > 0:
            time.sleep(_SUBMIT_STAGGER_SECONDS)
        procs[topic] = _submit(app_path, topic, cores, memory_mb)
        spawned_at[topic] = time.time()
    logger.info(f"submitted {len(procs)} bronze job(s), {cores} core(s)/{memory_mb}m each")

    respawn_at: "dict[str, float]" = {}
    try:
        while True:
            for topic, proc in list(procs.items()):
                if proc is None:
                    if time.time() >= respawn_at[topic]:
                        procs[topic] = _submit(app_path, topic, cores, memory_mb)
                        spawned_at[topic] = time.time()
                    continue
                code = proc.poll()
                if code is None:
                    continue
                lifetime = time.time() - spawned_at[topic]
                if lifetime < _FAST_FAILURE_THRESHOLD_SECONDS:
                    consecutive_fast_failures[topic] = consecutive_fast_failures.get(topic, 0) + 1
                else:
                    consecutive_fast_failures[topic] = 0
                backoff = min(
                    _RESPAWN_BACKOFF_SECONDS * (2 ** consecutive_fast_failures[topic]),
                    _MAX_RESPAWN_BACKOFF_SECONDS,
                )
                logger.error(
                    f"bronze job topic={topic} exited ({code}) after {lifetime:.0f}s - "
                    f"respawning in {backoff}s (consecutive fast failures={consecutive_fast_failures[topic]})"
                )
                procs[topic] = None
                respawn_at[topic] = time.time() + backoff
            time.sleep(_SUPERVISE_POLL_SECONDS)
    finally:
        for proc in procs.values():
            if proc is not None and proc.poll() is None:
                proc.terminate()
