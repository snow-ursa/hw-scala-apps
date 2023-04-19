package app

import java.sql.Timestamp

import org.apache.spark.sql.{DataFrame, Dataset, SaveMode, Row}
import org.apache.spark.sql.functions.{broadcast, col, count, max, mean, min, stddev}

import SparkCommonFunctions._

object HW3_3_Dataset extends App {

  val srcTaxiPath = "src/main/resources/data/yellow_taxi_jan_25_2018"
  val srcTaxiZonesPath = "src/main/resources/data/taxi_zones.csv"

  val dbHost = "localhost:5432"
  val dbName = "pg_test_db"
  val url = s"jdbc:postgresql://$dbHost/$dbName"
  val dbTable = "distance_distribution"
  val dbUser = "docker"

  implicit val spark = initSparkSession("Spark_Data_API_App")

  import spark.implicits._

  val taxiZonesDF: DataFrame = readCSV(srcTaxiZonesPath)

  case class TaxiTrip(VendorID: Int,
                      tpep_pickup_datetime: Timestamp,
                      tpep_dropoff_datetime: Timestamp,
                      passenger_count: Int,
                      trip_distance: Double,
                      RatecodeID: Int,
                      store_and_fwd_flag: String,
                      PULocationID: Int,
                      DOLocationID: Int,
                      payment_type: Int,
                      fare_amount: Double,
                      extra: Double,
                      mta_tax: Double,
                      tip_amount: Double,
                      tolls_amount: Double,
                      improvement_surcharge: Double,
                      total_amount: Double)

  val taxiDF: DataFrame = readParquet(srcTaxiPath)
  val taxiDS: Dataset[TaxiTrip] = taxiDF.as[TaxiTrip]

  val resultDS: Dataset[Row] =
    taxiDF
      .join(broadcast(taxiZonesDF), taxiDF("DOLocationID") === taxiZonesDF("LocationID"), "inner")
      .groupBy(col("Borough").alias("borough"))
      .agg(
        count("*").alias("trips_count"),
        mean(col("trip_distance")).alias("mean_distance"),
        stddev(col("trip_distance")).alias("std_distance"),
        min(col("trip_distance")).alias("min_distance"),
        max(col("trip_distance")).alias("max_distance"),
      )
      .orderBy(col("trips_count").desc)

  resultDS.toDF.write.format("jdbc")
    .mode(SaveMode.Append)
    .option("url", url)
    .option("user", dbUser)
    .option("password", dbUser)
    .option("dbtable", dbTable)
    .option("driver", "org.postgresql.Driver")
    .save()

}
