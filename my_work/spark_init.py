# spark_init.py
#
# Reusable SparkSession starter for local dev on your Lenovo / desktop.
# Import this in any notebook or script:
#
#   from spark_init import start_spark
#   spark = start_spark()
#   spark.range(10).show()
#

from pyspark.sql import SparkSession
import os, sys

def start_spark(
    app_name: str = "TDG-Optimized-Laptop",
    driver_memory: str = "12g",
    shuffle_partitions: str = "8",
    default_parallelism: str = "8",
    arrow_enabled: bool = True,
    python_executable: str | None = None,
):
    """
    Start or get a SparkSession tuned for a single machine with ~24GB RAM.

    Parameters
    ----------
    python_executable : str or None
        Path to the python executable used for PySpark workers. If None, uses
        the current interpreter (sys.executable). On Windows this avoids trying
        to invoke "python3".
    """
    # ensure worker/driver python are set before SparkSession creation
    if python_executable is None:
        python_executable = sys.executable

    os.environ.setdefault("PYSPARK_PYTHON", python_executable)
    os.environ.setdefault("PYSPARK_DRIVER_PYTHON", python_executable)

    builder = (
        SparkSession.builder
        .appName(app_name)
        .master("local[*]")  # use all cores
        .config("spark.driver.memory", driver_memory)
        .config("spark.sql.shuffle.partitions", shuffle_partitions)
        .config("spark.default.parallelism", default_parallelism)
        # set pyspark python in Spark conf as well
        .config("spark.pyspark.python", python_executable)
        .config("spark.pyspark.driver.python", python_executable)
    )

    if arrow_enabled:
        builder = builder.config(
            "spark.sql.execution.arrow.pyspark.enabled", "true"
        )

    spark = builder.getOrCreate()
    # keep logs readable
    spark.sparkContext.setLogLevel("WARN")
    return spark
