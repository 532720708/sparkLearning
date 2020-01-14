package cn.downey.spark.core

import org.apache.spark.{SparkConf, SparkContext}

object CheckPoint {

  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setMaster("local[*]").setAppName("CheckPoint")

    val sc = new SparkContext(conf)

    sc.setCheckpointDir("cp")

    val rdd = sc.makeRDD(List(1, 2, 3, 4))

    val mapRDD = rdd.map((_, 1))

    mapRDD.checkpoint()

    val reduceRDD = mapRDD.reduceByKey(_ + _)

    reduceRDD.foreach(println)
    println(reduceRDD.toDebugString)

    sc.stop()
  }

}
