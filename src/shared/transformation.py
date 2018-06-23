from src.utils.read_from_db import (
    read_in_dataframe,
    file_exits
)
from src.shared.exceptions import FileNotFoundException
import os


def create_transformed_file(spark, config, fs, incoming_file):
    source_file = config["source_file"]["incoming_file"]
    source_file = os.path.join(source_file, incoming_file)

    if not file_exits(spark, fs, source_file):
        message = "Incoming file nor found: {}".format(source_file)
        raise FileNotFoundException(message)

    source_df = read_in_dataframe(spark, source_file, header=True)
    source_df =  source_df.withColumnRenamed("lat", "Latitude")
    source_df.show()



