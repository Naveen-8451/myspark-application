from src.shared.transformation import (
    create_transformed_file,
    database_data
)


def main(initializer, source, db_type=None):
    """

    :param initializer:
    :return:
    """
    logger = initializer.LOGGER
    spark = initializer.spark
    config = initializer.config
    fs = spark._jvm.org.apache.hadoop.fs.FileSystem.get(spark._jsc.hadoopConfiguration())
    try:
        if source == "db":
            print("Read Data from DB")
            _ = database_data(spark, config, fs, db_type)
        elif source == "file":
            _ = create_transformed_file(spark, config, fs, source)
    except Exception as e:
        logger.error(str(e))
        raise Exception(str(e))
    finally:
        print("Hello")
        spark.stop()

