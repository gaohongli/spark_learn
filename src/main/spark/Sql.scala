import org.apache.spark.sql.functions.countDistinct
import org.apache.spark.sql.types.{IntegerType, StringType, StructField, StructType}
import org.apache.spark.sql.{Row, SparkSession}

object Sql {
  def main(args: Array[String]): Unit = {
    //Test.hive()
    Sql.UVStatistic()
  }

  def hive(): Unit = {
    val spark = SparkSession.builder().appName("hive").enableHiveSupport().getOrCreate()
    spark.sql("select * from test.word_count")
  }

  def UVStatistic(): Unit = {
    val spark = SparkSession.builder().appName("test").master("local[*]").getOrCreate();
    val arr = Array(
      "2019-06-01,0001",
      "2019-06-01,0004",
      "2019-06-01,0005",
      "2019-06-01,0003",
      "2019-06-01,0003",
      "2019-06-02,0005",
      "2019-02-01,0001"
    )

    //转为RDD[Row]
    var rowRDD = spark.sparkContext.makeRDD(arr).map(line =>
      Row(line.split(",")(0), line.split(",")(1).toInt)
    )

    //构建DataFrame元数据
    var structType = StructType(Array(
      StructField("date", StringType, true),
      StructField("userid", IntegerType, true)
    ))

    //将RDD【Row】转为DataFrame
    val df = spark.createDataFrame(rowRDD, structType)

    //聚合查询
    //根据日期分组，然后将每一组用户ID去重后统计数量
    df.groupBy("date").agg(countDistinct("userid") as "count").show()
  }
}
