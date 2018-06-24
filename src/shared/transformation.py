from src.utils.read_from_db import (
    read_in_dataframe,
    file_exits
)
from src.shared.exceptions import FileNotFoundException
import os


def create_transformed_file(spark, config, fs, incoming_file):
    source_file = config["source_file"]["incoming_file"]
    source_file = os.path.join(source_file, incoming_file)
    target_file = config["target"]["target_file"]
    stg_target_file = config["target"]["target_stg"]

    if not file_exits(spark, fs, source_file):
        message = "Incoming file nor found: {}".format(source_file)
        raise FileNotFoundException(message)

    source_df = read_in_dataframe(spark, source_file, header=True)
    source_df.write.partitionBy("zip").format("parquet").mode("overwrite").save(stg_target_file)





