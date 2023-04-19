package app

import scala.util.{Try, Failure, Success}

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions.{broadcast, col, count}

import SparkCommonFunctions._

object HW3_1_DataFrame extends App {

  val srcTaxiPath = "src/main/resources/data/yellow_taxi_jan_25_2018"
  val srcTaxiZonesPath = "src/main/resources/data/taxi_zones.csv"
  val dstPath = "src/main/resources/result/hw3_1/yellow_taxi_top_trips_count"

  implicit val spark = initSparkSession("Spark_Data_API_App")

  val taxiDF: DataFrame = readParquet(srcTaxiPath)
  val taxiZonesDF: DataFrame = readCSV(srcTaxiZonesPath)

  val result: DataFrame =
    taxiDF
      .join(broadcast(taxiZonesDF), taxiDF("DOLocationID") === taxiZonesDF("LocationID"), "inner")
      .groupBy(col("Borough"))
      .agg(count("*").as("TripsCount"))
      .orderBy(col("TripsCount").desc)

  result.show()

  Try(writeParquet(result, dstPath)) match {
    case Success(value) => println("Parquet was saved successfully")
    case Failure(exception) => println(s"Something went wrong while writing parquet: ${exception.printStackTrace()}")
  }

  //  spark.read.parquet(dstPath).show()
}
