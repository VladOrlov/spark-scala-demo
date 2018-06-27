package com.jvosoft.sparkscalatutorial.ssqljobs

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession
import org.springframework.beans.factory.annotation.Value
import org.springframework.scheduling.annotation.Scheduled
import org.springframework.stereotype.Component


@Component
class CityProcessing(@Value("${dfjobs.city.active}") active: Boolean,
                     @Value("${dfjobs.city.path}") filePath: String) extends Serializable {

  @Scheduled(cron = "${jobs.default-schedule}")
  def execute(): Unit = {
    if (!active) {
      return
    }
    Logger.getLogger("org").setLevel(Level.ERROR)
    println("Job executed: CityProcessing")

    val spark = SparkSession.builder()
      .appName("spark-tutorial")
      .master("local[*]")
      .getOrCreate()

    // Create a DataFrame from Spark Session read csv
    // Technically known as class Dataset
    val df = spark.read.option("header", "true").option("inferSchema", "true").csv(filePath)

    import spark.implicits._
    //    df.filter($"Close" < 480 && $"High" < 480).show()
    df.filter($"Close" === 480).show()
    //    df.filter("Close < 480 AND High < 480").show()

    //    // Get first 5 rows
    //    df.head(5) // returns an Array
    //    println("\n")
    //    for (line <- df.head(10)) {
    //      println(line)
    //    }
    //
    //    // Get column names
    //    val columns = df.columns
    //    for (columnName <- columns) {
    //      println(columnName)
    //    }
    //
    //    // Find out DataTypes
    //    // Print Schema
    //    df.printSchema()
    //
    //    // Describe DataFrame Numerical Columns
    //    df.describe()
    //
    //    // Select columns .transform().action()
    //    df.select("Volume").show()
    //
    //    // Multiple Columns
    //    df.select("Date", "Close").show(2)
    //
    //    // Creating New Columns
    //    val df2 = df.withColumn("HighPlusLow", df("High") - df("Low"))
    //    // Show result
    //    df2.columns
    //    df2.printSchema()
    //
    //    // Recheck Head
    //    df2.head(5)
    //
    //    // Renaming Columns (and selecting some more)
    //    df2.select(df2("HighPlusLow").as("HPL"), df2("Close")).show()

  }
}
