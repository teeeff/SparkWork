package PlayGround

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types.{DateType, DoubleType, LongType, StringType, StructField, StructType}

object PlayGround2 {

  def main(args: Array[String]): Unit = {
    println("hi here")
    val spark = SparkSession.builder()
      .appName("Spark Essentials Playground App")
      .config("spark.master", "local")
      .getOrCreate()

    val carsSchema = StructType(Array(
      StructField("Name", StringType),
      StructField("Miles_per_Gallon", DoubleType),
      StructField("Cylinders", LongType),
      StructField("Displacement", DoubleType),
      StructField("Horsepower", LongType),
      StructField("Weight_in_lbs", LongType),
      StructField("Acceleration", DoubleType),
      StructField("Year", DateType),
      StructField("Origin", StringType)
    ))

    val carsDF = spark.read
      .format("json")
      .schema(carsSchema) // enforce a schema
      .option("dateFormat", "YYYY-MM-dd")
      .option("mode", "failFast") // dropMalformed, permissive (default)
      .option("path", "src/main/resources/data/cars.json")
      .load()


carsDF.show(10)



  }


}
