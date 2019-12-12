package cn.downey.spark.sql

import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

object SparkSQL01_Demo {
  def main(args: Array[String]): Unit = {
    //SparkSQL

    //SparkConf 配置对象
    val sparkConf = new SparkConf().setMaster("local[*]").setAppName(this.getClass.getName)

    //SparkContext

    //SparkSession SparkSQL的环境对象
    val spark = SparkSession.builder().config(sparkConf).getOrCreate()

    //读取数据，构建DataFrame
    val frame = spark.read.json("in/user.json")

    frame.show()

    spark.stop()

  }

}
