import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.spark.streaming.{Seconds, StreamingContext}

import scala.collection.mutable

object Streaming {
  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setMaster("local[2]").setAppName("QueueStream")
    val ssc = new StreamingContext(sparkConf, Seconds(1))
    //创建队列，用于存放RDD
    val rddQueue = new mutable.Queue[RDD[Int]]()
    //创建输入DStream
    val inputStream = ssc.queueStream(rddQueue)
    val mappedStream = inputStream.map(x => (x % 10, 1))
    val reduceStream = mappedStream.reduceByKey(_ + _)
    reduceStream.print()
    ssc.start()

    //每隔1秒创建一个RDD并将其推入队列rddQueue中国
    for (i <- 1 to 30) {
      rddQueue.synchronized {
        rddQueue += ssc.sparkContext.makeRDD(1 to 1000, 10)
      }
      Thread.sleep(1000)
    }
    ssc.stop()
  }
}
