package cn.downey.spark.core

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object WordCount {
  def main(args: Array[String]): Unit = {

    //使用开发工具完成Spark WordCount的开发

    //local模式
    //创建SparkConf对象
    //设定Spark计算框架的运行(部署)环境
    val config : SparkConf = new SparkConf().setMaster("local[*]").setAppName("WordCount")
    //创建Spark上下文对象
    val sc = new SparkContext(config)

    //读取文件,将文件内容一行一行地读取
    //路径查找位置默认从当前的部署环境中查找
    //如果需要从本地查找，file:///opt/module/spark/in
    val lines : RDD[String] = sc.textFile("in/word.txt")

    //将一行一行的数据分解成一个一个单词
    val words: RDD[String] = lines.flatMap(_.split(" "))

    //为了统计方便，将单词数据进行结构转换
    val wordToOne: RDD[(String, Int)] = words.map((_, 1))

    //对转换结构后的数据进行分组聚合
    val wordToSum: RDD[(String, Int)] = wordToOne.reduceByKey(_ + _)

    //将统计结果打印到控制台
    val result: Array[(String, Int)] = wordToSum.collect()

    result.foreach(println)
  }
}
