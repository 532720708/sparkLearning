package cn.downey.spark.sql

import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, Dataset, Row, SparkSession}

object SparkSQL03_Transform {
  def main(args: Array[String]): Unit = {
    //SparkSQL


    val sparkConf = new SparkConf().setMaster("local[*]").setAppName(this.getClass.getName)

    val spark = SparkSession.builder().config(sparkConf).getOrCreate()

    //隐式转换
    //这里的spark不是包名, 是SparkSession对象的名字
    import spark.implicits._

    //创建RDD
    val rdd = spark.sparkContext.makeRDD(List((1, "zhangsan", 20), (2, "lisi", 30), (3, "wangwu", 40)))

    //转换为DF
    val df = rdd.toDF("id", "name", "age")

    //转换为DS
    val ds: Dataset[User] = df.as[User]

    //转换为DF
    val df1: DataFrame = ds.toDF()

    //转换为RDD
    val rdd1: RDD[Row] = df1.rdd

    rdd1.foreach(row => {
      println(row.getString(1))
    })

    spark.stop()

  }

}

case class User(id: Int, name: String, age: Int)
