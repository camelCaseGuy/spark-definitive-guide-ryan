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


def start_spark(
    app_name: str = "TDG-Optimized-Laptop",
    driver_memory: str = "8g",
    shuffle_partitions: str = "8",
    default_parallelism: str = "8",
    arrow_enabled: bool = True,
):
    """
    Start or get a SparkSession tuned for a single machine with ~24GB RAM.

    Parameters
    ----------
    app_name : str
        Name shown in Spark UI.
    driver_memory : str
        Memory for the driver, e.g. "4g", "8g".
    shuffle_partitions : str
        Number of shuffle partitions (default 200 is overkill for local).
    default_parallelism : str
        Default parallelism for RDD ops in local mode.
    arrow_enabled : bool
        Use Arrow for toPandas() conversions (faster).

    Returns
    -------
    spark : pyspark.sql.SparkSession
    """
    builder = (
        SparkSession.builder
        .appName(app_name)
        .master("local[*]")  # use all cores
        .config("spark.driver.memory", driver_memory)
        .config("spark.sql.shuffle.partitions", shuffle_partitions)
        .config("spark.default.parallelism", default_parallelism)
    )

    if arrow_enabled:
        builder = builder.config(
            "spark.sql.execution.arrow.pyspark.enabled", "true"
        )

    spark = builder.getOrCreate()
    # keep logs readable
    spark.sparkContext.setLogLevel("WARN")
    return spark
