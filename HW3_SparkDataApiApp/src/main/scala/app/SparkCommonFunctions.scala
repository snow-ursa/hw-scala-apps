package app

import org.apache.spark.sql.{DataFrame, SparkSession}

object SparkCommonFunctions {
  def initSparkSession(appName: String): SparkSession = {
    SparkSession
      .builder()
      .appName(appName)
      .config("spark.master", "local")
      .getOrCreate()
  }
  def readParquet(path: String)(implicit spark: SparkSession): DataFrame = spark.read.parquet(path)
  def readCSV(path: String)(implicit spark: SparkSession): DataFrame = {
    spark
      .read
      .option("header", "true")
      .option("inferSchema", "true")
      .csv(path)
  }
  def writeParquet(df: DataFrame, path: String, coalesce: Int = 1) = {
    df.coalesce(coalesce).write.mode("overwrite").parquet(path)
  }

}
