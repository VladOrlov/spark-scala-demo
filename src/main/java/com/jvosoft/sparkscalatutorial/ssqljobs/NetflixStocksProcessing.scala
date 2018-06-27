package com.jvosoft.sparkscalatutorial.ssqljobs

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession
import org.springframework.beans.factory.annotation.Value
import org.springframework.scheduling.annotation.Scheduled
import org.apache.spark.sql.functions.year
import org.springframework.stereotype.Component

@Component
class NetflixStocksProcessing(@Value("${dfjobs.netflix.active}") active: Boolean,
                              @Value("${dfjobs.netflix.path}") filePath: String) extends Serializable {


  @Scheduled(cron = "${jobs.default-schedule}")
  def execute(): Unit = {
    if (!active) {
      return
    }
    Logger.getLogger("org").setLevel(Level.ERROR)
    println("Job executed: CityProcessing")

    val session = SparkSession.builder()
      .appName("spark-tutorial")
      .master("local[*]")
      .getOrCreate()

    val netflixStocksDS = session.read.option("header", "true").option("inferSchema", "true").csv(filePath)

    val yearDf = netflixStocksDS.withColumn("Year", year(netflixStocksDS("Date")))
    yearDf.select("Year", "High").groupBy("Year").max().orderBy("Year").show()

  }
}
