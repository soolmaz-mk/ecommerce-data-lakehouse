# Small launcher for the Spark maintenance job.
# spark-submit needs a plain script path, not a package.
import sys

sys.path.insert(0, "/opt/spark-apps")

from jobs.iceberg_maintenance.main import main  # noqa: E402

if __name__ == "__main__":
    sys.exit(main())
