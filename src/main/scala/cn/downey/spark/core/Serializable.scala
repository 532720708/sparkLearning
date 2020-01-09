package cn.downey.spark.core

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object Serializable {
  def main(args: Array[String]): Unit = {

    val config = new SparkConf().setMaster("local[*]").setAppName("Serializable")
    val sc = new SparkContext(config)

    val rdd = sc.parallelize(Array("hadoop", "spark", "kafka", "hive"))

    //driver中执行，但rdd中算子是由executor执行
    val search = new Search("h")

    val match1 = search.getMatch1(rdd)
    match1.collect().foreach(println)

    //    val match2 = search.getMatch2(rdd)
    //    match2.collect().foreach(println)


    sc.stop()
  }
}

class Search(query: String) extends java.io.Serializable {

  //过滤出包含字符串的数据
  def isMatch(s: String) = {
    s.contains(query)
  }

  //过滤出包含字符串的RDD（成员方法）
  def getMatch1(rdd: RDD[String]) = {
    rdd.filter(isMatch)
  }

  //过滤出包含字符串的RDD（匿名函数）
  def getMatch2(rdd: RDD[String]) = {
    //    val q = query
    //    rdd.filter(x => x.contains(q))
    rdd.filter(x => x.contains(query))
  }

}


