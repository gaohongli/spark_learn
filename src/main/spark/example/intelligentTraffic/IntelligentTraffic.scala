package example.intelligentTraffic

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{Row, SparkSession}
import util.Common

object IntelligentTraffic {
  val spark: SparkSession = Common.getSparkSession() //构建SparkSession

  def main(args: Array[String]): Unit = {
    //initDB()
    crossingTop()
  }

  /*
  * 初始化数据库
  */
  def initDB() {
    System.setProperty("HADOOP_USER_NAME", "root")
    //构建SparkSession
    //使用数据库traffic_db
    spark.sql("USE traffic_db");
    spark.sql("DROP TABLE IF EXISTS monitor_flow_action");
    //在hive中创建monitor_flow_action表
    spark.sql("CREATE TABLE IF NOT EXISTS monitor_flow_action " +
      "(date STRING,monitor_id STRING,camera_id STRING,car STRING," +
      "action_time STRING,speed STRING,road_id STRING,area_id STRING) " +
      "row format delimited fields terminated by '\t' ")
    //导入数据到表monitor_flow_action
    spark.sql("load data local inpath " +
      "'/root/tmp/monitor_flow_action' " +
      "into table monitor_flow_action")

    //在hive中创建monitor_camera_info表
    spark.sql("DROP TABLE IF EXISTS monitor_camera_info")
    spark.sql("CREATE TABLE IF NOT EXISTS monitor_camera_info " +
      "(monitor_id STRING, camera_id STRING) " +
      "row format delimited fields terminated by '\t'")
    //导入数据到表monitor_camera_info
    spark.sql("LOAD DATA LOCAL INPATH " +
      "'/root/tmp/monitor_camera_info' " +
      "INTO TABLE monitor_camera_info")

    System.out.println("========data2hive finish========")
  }

  /*
  * 能告诉通过的路口top5
  * */
  def crossingTop(): Unit = {
    //使用数据库traffic_db
    spark.sql("USE traffic_db");
    //1. 将表monitor_flow_action的数据转为RDD[Row]
    val monitorFlowRDD: RDD[Row] = spark.sql("select * from monitor_flow_action").rdd
    //2. 将RDD[Row]转为RDD[(String, Row)]，String为monitor_id
    val monitorFlowRDDKV: RDD[(String, Row)] =
      monitorFlowRDD.map(row => (row(1).toString, row))
    //3. 将RDD[(String, Row)]按照key进行分组，即按照卡口号monitor_id分组，
    //每个卡口号对应多个Row
    val groupByMonitorIdRDD: RDD[(String, Iterable[Row])] =
    monitorFlowRDDKV.groupByKey()

    //4. 将RDD[(String, Iterable[Row])]转为RDD[(SpeedSortKey, String)]
    //SpeedSortKey为自定义排序类，存储每个卡口高速、中速、正常、低速通过的车辆数量
    val sortKeyRDD: RDD[(SpeedSortKey, String)] = groupByMonitorIdRDD.map(line
    => {
      val monitorId: String = line._1
      val speedIterator: Iterator[Row] = line._2.iterator
      //统计各类速度的车辆数量
      var lowSpeedCount = 0
      var normalSpeedCount = 0
      var mediumSpeedCount = 0
      var highSpeedCount = 0
      while (speedIterator.hasNext) {
        val speed = speedIterator.next.getString(5).toInt
        if (speed >= 0 && speed < 60) lowSpeedCount += 1
        else if (speed >= 60 && speed < 90) normalSpeedCount += 1
        else if (speed >= 90 && speed < 120) mediumSpeedCount += 1
        else if (speed >= 120) highSpeedCount += 1
      }
      //将各类速度的车辆数量存入自定义排序类SpeedSortKey
      val speedSortKey = new SpeedSortKey(lowSpeedCount, normalSpeedCount,
        mediumSpeedCount, highSpeedCount)
      (speedSortKey, monitorId)
    })
    //5. 根据key降序排列
    val sortResult: RDD[(SpeedSortKey, String)] = sortKeyRDD.sortByKey(false)
    //6. 取前5个
    val result: Array[(SpeedSortKey, String)] = sortResult.take(5)
    //7. 打印结果
    result.foreach(line => println("monitor_id = " + line._2 + "-------" + line._1))
  }
}
