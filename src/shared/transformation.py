from src.utils.read_file_formats import (
    read_in_dataframe,
    read_table_from_db,
    file_exits
)
from src.shared.exceptions import(
    FileNotFoundException,
    DataBaseTypeNotFound,
    TableNotFoundException,
    CredentialNotFoundException
)
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


def database_data(spark, config, fs, db_type):
    table_name = config["db_table"]["table_names"]
    target_file = config["target"]["target_file"]
    stg_target_file = config["target"]["target_stg"]

    if db_type == "mysql":
        driver = "com.mysql.jdbc.Driver"
        url = config["db_type_url"]["mysql"]
        user_name = config["db_credential"]["user_name"]
        password = config["db_credential"]["password"]

        for table_name in table_name:
            if table_name == "":
                message = "Please provide SourceTable Name: {}".format(table_name)
                raise TableNotFoundException(message)
            elif user_name == "" and password == "":
                message = "Please provide SourceTable Name: {}".format(user_name)
                raise CredentialNotFoundException(message)
            else:
                source_table_df = read_table_from_db(spark, url, driver, table_name, user_name, password)
                source_table_df.coalesce(1).write.mode("overwrite").save(os.path.join(stg_target_file, table_name))

    elif db_type == "db2":
        driver = ""
        if table_name == "":
            message = "Please provide SourceTable Name: {}".format(table_name)
            raise TableNotFoundException(message)
        else:
            ""
    elif db_type == "sql-server":
        driver = ""
        if table_name == "":
            message = "Please provide SourceTable Name: {}".format(table_name)
            raise TableNotFoundException(message)
        else:
            ""

    elif db_type == "hive":
        driver = ""
        if table_name == "":
            message = "Please provide SourceTable Name: {}".format(table_name)
            raise TableNotFoundException(message)
        else:
            ""
    else:
        message = "Please provide Database Type: {}".format(db_type)
        raise DataBaseTypeNotFound(message)





