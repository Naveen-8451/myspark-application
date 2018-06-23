from src.shared.transformation import create_transformed_file


def main(initializer, source_file, ):
    """

    :param initializer:
    :return:
    """
    logger = initializer.LOGGER
    spark = initializer.spark
    config = initializer.config
    fs = spark._jvm.org.apache.hadoop.fs.FileSystem.get(spark._jsc.hadoopConfiguration())
    try:
        _ = create_transformed_file(spark, config, fs, source_file)
    except Exception as e:
        logger.error(str(e))
        raise Exception(str(e))
    finally:
        spark.stop()

