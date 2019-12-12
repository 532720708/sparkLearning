package cn.downey.spark.sql

import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

object SparkSQL02_SQL {
  def main(args: Array[String]): Unit = {
    //SparkSQL

    val sparkConf = new SparkConf().setMaster("local[*]").setAppName(this.getClass.getName)

    val spark = SparkSession.builder().config(sparkConf).getOrCreate()

    //读取数据，构建DataFrame
    val frame = spark.read.json("in/user.json")

    //将DataFrame转换为一张表
    frame.createOrReplaceTempView("user")

    //采用SQL语法访问数据
    spark.sql("select * from user").show()

    spark.stop()

  }

}
