package retail_db

import org.apache.spark.{SparkConf, SparkContext}
import java.io._

import org.apache.spark.sql.SparkSession

object GetRevenuePerOrder {
  val inputPath = "/Users/tinufarid/Downloads/data-master/retail_db/order_items/part-00000"
  val outputPath = "/Users/tinufarid/Downloads/data-master/output"

  def main(args: Array[String]): Unit = {
    //val conf = new SparkConf().setMaster(args(0)).clone()setAppName("Get revenue per order")
    //val conf = new SparkConf().setMaster("local").clone()setAppName("Get revenue per order")
    //conf.getAll.foreach(println)
    //val sc = new SparkContext(conf)
    //sc.setLogLevel("ERROR")
    //val orderItems = sc.textFile(args(1))

    val spark = SparkSession.builder()
      .appName("Spark Essentials Playground App")
      .config("spark.master", "local")
      .getOrCreate()

    val orderItems = spark.read.option("header", "false").csv(inputPath)

  println(orderItems.count())
    //revenuePerOrder.saveAsTextFile(args(2))
    val file: File = new File(outputPath)
    println("Inside the program")
    deleteRecursively(file)
  }

  def deleteRecursively(file: File): Unit = {
    if (file.isDirectory) {
      file.listFiles.foreach(deleteRecursively)
    }
    if (file.exists && !file.delete) {
      throw new Exception(s"Unable to delete ${file.getAbsolutePath}")
    }
  }

}
