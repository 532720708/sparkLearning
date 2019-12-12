package cn.downey.spark.core

import org.apache.spark.{SparkConf, SparkContext}

object Spark12_Oper11 {

  def main(args: Array[String]): Unit = {
    val config : SparkConf = new SparkConf().setMaster("local[*]").setAppName("WordCount")

    //创建Spark上下文对象
    val sc = new SparkContext(config)


    val listRDD = sc.makeRDD((List(1, 2, 3, 4, 5, 6, 7, 1, 6)))

//    listRDD.coalesce()
  }
}
