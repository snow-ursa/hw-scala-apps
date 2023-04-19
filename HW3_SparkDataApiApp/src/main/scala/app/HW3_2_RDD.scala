package app

import java.sql.Timestamp
import java.time.format.DateTimeFormatter
import java.time.{LocalDateTime, LocalTime}
import java.io._

import org.apache.spark.sql.{DataFrame, Dataset}
import org.apache.spark.rdd.RDD

import SparkCommonFunctions._


object HW3_2_RDD extends App {
  val srcTaxiPath = "src/main/resources/data/yellow_taxi_jan_25_2018"
  val dstPath = "src/main/resources/result/hw3_2/yellow_taxi_top_popular_time.txt"

  implicit val spark = initSparkSession("Spark_Data_API_App")

  import spark.implicits._

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

  implicit val formatter = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss.S")

  private def prepareStamp(stamp: String)(implicit formatter: DateTimeFormatter): LocalTime = {
    LocalDateTime.parse(stamp, formatter).toLocalTime
  }

  val taxiDF: DataFrame = readParquet(srcTaxiPath)
  val taxiDS: Dataset[TaxiTrip] = taxiDF.as[TaxiTrip]
  val taxiRDD: RDD[TaxiTrip] = taxiDS.rdd
  //  val taxiRDD = taxiDF.rdd

  val resultRDD: RDD[(LocalTime, Int)] = taxiRDD
    .map(x => (prepareStamp(x.tpep_pickup_datetime.toString), 1))
    .reduceByKey(_ + _)
    .sortBy(_._2, ascending = false)

  val prWriter = new PrintWriter(new File(dstPath))
  resultRDD.foreach(x => {
    val elem = s"${x._1} ${x._2}"
    println(elem)
    prWriter.append(s"$elem\n")
  })
  prWriter.close

}
