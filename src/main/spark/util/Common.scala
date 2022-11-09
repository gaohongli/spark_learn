package util

import org.apache.spark.sql.SparkSession

object Common {
  def getSparkSession(): SparkSession = {
    return SparkSession.builder().appName("Data2Hive")
      .master("spark://master:7077")
      .config("spark.sql.warehouse.dir", "hdfs://master:9000/user/hive/warehouse")
      .enableHiveSupport() //开启Hive支持
      .getOrCreate()
  }
}
