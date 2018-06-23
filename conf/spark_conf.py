from pyspark import SparkSession

# should be made Generic to pass this as a argument to the spark.
job_name = "Spark Application"


def create_spark_session():
    spark = get_spark_session(job_name)

def get_spark_session(jon_name):
    SparkSession.builder.master("local")\
        .appName(job_name)\
        .config("spark.some.config.option", "some-value")\
        .getOrCreate()

