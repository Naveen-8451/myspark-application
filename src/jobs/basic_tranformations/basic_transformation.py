from src.shared.transformation import create_transformed_file


def main(initializer):
    """

    :param initializer:
    :return:
    """
    logger = initializer.LOGGER
    spark = initializer.spark
    config = initializer.config

    try:
        _ = create_transformed_file()
    except Exception as e:
        logger.error(str(e))
        raise Exception(str(e))
    finally:
        spark.stop()

