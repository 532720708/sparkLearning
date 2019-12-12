package cn.downey.spark.streaming

import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.{DStream, ReceiverInputDStream}
import org.apache.spark.streaming.{Duration, Seconds, StreamingContext}

object StreamWordCount {
  def main(args: Array[String]): Unit = {

    //使用SparkStreming完成wordCount

    val sparkConf = new SparkConf().setMaster("local[*]").setAppName(this.getClass.getName)

    //实时数据分析环境对象
    //采集周期，以指定时间为周期采集实时数据
    val streamingContext = new StreamingContext(sparkConf, Seconds(3))

    val socketLineDStream: ReceiverInputDStream[String] = streamingContext.socketTextStream("hadoop103", 9999)

    //将采集的数据进行分解(扁平化)
    val wordDStream: DStream[String] = socketLineDStream.flatMap(line => line.split(" "))

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
