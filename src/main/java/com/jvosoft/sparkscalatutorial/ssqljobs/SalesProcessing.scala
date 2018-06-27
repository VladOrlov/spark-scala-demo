package com.jvosoft.sparkscalatutorial.ssqljobs

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession
import org.springframework.beans.factory.annotation.Value
import org.springframework.scheduling.annotation.Scheduled
import org.springframework.stereotype.Component

@Component
class SalesProcessing(@Value("${dfjobs.sales.active}") active: Boolean,
                      @Value("${dfjobs.sales.path}") filePath: String) extends Serializable {

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

    df.groupBy("Company").mean().show()
    df.groupBy("Company").min().show()
    df.groupBy("Company").max().show()
    df.groupBy("Company").sum().show()
  }
}
