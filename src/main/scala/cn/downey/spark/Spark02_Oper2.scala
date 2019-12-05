package cn.downey.spark

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object Spark02_Oper2 {
  def main(args: Array[String]): Unit = {

    val config : SparkConf = new SparkConf().setMaster("local[*]").setAppName("WordCount")

    //创建Spark上下文对象
    val sc = new SparkContext(config)

    // mapPartitions算子
    val listRDD: RDD[Int] = sc.makeRDD(1 to 10)

    // mapPartitions可以对一个RDD中所有的分区进行遍历
    val mapPartitionsRDD: RDD[Int] = listRDD.mapPartitions(datas => {
      datas.map(_*2)
    })

    mapPartitionsRDD.collect().foreach(println)


  }
}
