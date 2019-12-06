package cn.downey.spark

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 * 行动算子 Actions
 * 转换算子 Transformations
 */
object Spark02_Oper1 {
  def main(args: Array[String]): Unit = {

    val config : SparkConf = new SparkConf().setMaster("local[*]").setAppName("WordCount")

    //创建Spark上下文对象
    //只要创建Spark上下文对象，这个类就叫Driver！

    val sc = new SparkContext(config)

    // map算子
    val listRDD: RDD[Int] = sc.makeRDD(1 to 4)

    val i = 10
    //所有RDD算子当中的计算功能都是由Executor做的
    //i要能序列化
    val mapRDD: RDD[Int] = listRDD.map(_*i)

    mapRDD.collect().foreach(println)


  }
}
