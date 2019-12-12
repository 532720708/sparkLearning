package cn.downey.spark.streaming

import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.{DStream, ReceiverInputDStream}
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.streaming.kafka.KafkaUtils


object KafkaSparkStreaming {

  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setMaster("local[*]").setAppName(this.getClass.getName)

    val streamingContext = new StreamingContext(sparkConf, Seconds(3))

    //[atguigu@hadoop100 kafka]$ bin/kafka-topics.sh
    // --zookeeper hadoop100:2181
    // --create --topic kafkaStreamDemo
    // --partitions 4 --replication-factor 2
    val kafkaDStream: ReceiverInputDStream[(String, String)] = KafkaUtils.createStream(
      streamingContext,
      "hadoop100:2181,hadoop101:2181,hadoop102:2181,hadoop103:2181",
      "downey",
      Map("kafkaStreamDemo" -> 5)
    )

    //将采集的数据进行分解(扁平化)
    val wordDStream: DStream[String] = kafkaDStream.flatMap(t => t._2.split(" "))

    //将数据进行结构的转变，方便进行统计分析
    val mapDStream: DStream[(String, Int)] = wordDStream.map((_, 1))

    //将转换结构后的数据聚合处理
    val wordToSumDStream: DStream[(String, Int)] = mapDStream.reduceByKey(_ + _)

    //打印结果
    wordToSumDStream.print()

    //不能停止采集程序
    //    streamingContext.stop()

    //启动采集器
    streamingContext.start()
    //Driver等待采集器的执行
    streamingContext.awaitTermination()

  }
}
