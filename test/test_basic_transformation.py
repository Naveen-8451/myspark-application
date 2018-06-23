import pytest
from src.libs.pyspark_shared_libs_8451.shared.initializer import Initializer
from src.shared.exceptions import FileNotFoundException
from src.shared.transformation import create_transformed_file

@pytest.fixture(scope="module")
def create_initializer():
    return Initializer("basic_transformation Test", env="local")
def test_incoming_file_exists():
    init = create_initializer()
    spark = init.spark
    fs = spark._jvm.org.apache.hadoop.fs.FileSystem.get(spark._jsc.hadoopConfiguration())

    config = {
        "source_file": {
            "incoming_file": "test/test_data"
        }
    }
    bad_file = "bad.csv"
    with pytest.raises(FileNotFoundException):
        _ = create_transformed_file(spark, config, fs, bad_file)