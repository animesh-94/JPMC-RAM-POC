import os
import sys
from pyspark.sql import SparkSession

os.environ["PYSPARK_PYTHON"] = sys.executable
os.environ["PYSPARK_DRIVER_PYTHON"] = sys.executable
hadoop_home = r"C:\Users\ANIMESH\Desktop\paychase_poc\hadoop"
os.environ["HADOOP_HOME"] = hadoop_home
os.environ["PATH"] += os.pathsep + os.path.join(hadoop_home, "bin")

def get_spark():
    return (SparkSession.builder
            .master("local[*]")
            .appName("paychase-refined-poc")
            .config("spark.sql.shuffle.partitions", "4")
            .getOrCreate())
