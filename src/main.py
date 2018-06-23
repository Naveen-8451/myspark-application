from src.libs.pyspark_shared_libs_8451 import PySparkBaseApplication
from resources.config import app_config

if __name__ == "__main__":
    PySparkBaseApplication.run({
        'app_config':app_config,
        'io_enabled':True
    })