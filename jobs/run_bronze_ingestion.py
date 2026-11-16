"""Entry point for the bronze ingestion job — launcher or single-topic worker.

"""

import argparse
import os

if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Bronze ingestion: launcher (no --topic) or per-topic job (--topic)"
    )
    parser.add_argument(
        "--topic", help="ingest just this topic; omit to launch one job per topic"
    )
    args = parser.parse_args()
    if args.topic:
        # lazy import: main.py pulls in pyspark, only present under spark-submit
        from bronze_ingestion.main import run
        run(args.topic)
    else:
        # lazy import: launcher.py is pyspark-free, runs under plain python3
        from bronze_ingestion.launcher import submit_all
        submit_all(app_path=os.path.abspath(__file__))
