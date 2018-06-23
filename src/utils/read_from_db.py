def read_table_from_db(db_type):
    pass


def read_in_dataframe(spark, file_name, columns=None, format="csv", header=False, sep=","):
    """

    :param spark:
    :param file_name:
    :param columns:
    :param format:
    :param header:
    :param sep:
    :return:
    """
    if not format == "csv":
        print("format is not CSV")
    elif format == "csv":
        print("csv Fomat")
        source_df = spark.read.csv(file_name, header=header, sep=sep)
        return source_df

def file_exits(spark, fs, file_name):
   """

   :param spark:
   :param fs:
   :param file_name:
   :return:
   """
   return fs.exists(spark._jvm.org.apache.hadoop.fs.Path(file_name))


