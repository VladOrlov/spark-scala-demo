package com.jvosoft.sparkscalatutorial.jobs.weather

import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.springframework.beans.factory.annotation.Value
import org.springframework.scheduling.annotation.Scheduled
import org.springframework.stereotype.Component

import scala.math.{max, min}

@Component
class TemperatureProcessing(@Value("${jobs.weather.active}") active: Boolean,
                            @Value("${jobs.weather.filter-type}") filterType: String,
                            @Value("${jobs.weather.path}") filePath: String) extends Serializable {

  @Scheduled(cron = "${jobs.default-schedule}")
  def execute(): Unit = {
    if (!active) {
      return
    }
    Logger.getLogger("org").setLevel(Level.ERROR)
    println("Job executed: FriendsByAge")

    val sparkContext = new SparkContext("local[*]", "spark-tutorial")

    val stationTemperature = sparkContext.textFile(filePath)
      .map(parseLine)
      .filter(temp => temp._2 == filterType)
      .map(temp => (temp._1, temp._3.toFloat))

    val result = reduceData(stationTemperature).collect()

    for (t <- result.sorted) {
      val station = t._1
      val temp = t._2
      val formattedTemp = f"$temp%.2f F"
      println(s"$station minimum temperature: $formattedTemp")
    }
  }

  def parseLine(line: String): (String, String, Float) = {

    val fields = line.split(",")

    val stationId = fields(0)
    val entryTypes = fields(2)
    val temperature = fields(3).toFloat * 0.1f * (9.0f / 5.0f) + 32.0f

    (stationId, entryTypes, temperature)
  }

  def reduceData(stationTemperature: RDD[(String, Float)]): RDD[(String, Float)] = {
    if (filterType == "TMIN") {
      stationTemperature.reduceByKey((x, y) => min(x, y))
    } else if (filterType == "TMAX") {
      stationTemperature.reduceByKey((x, y) => max(x, y))
    } else {
      stationTemperature
    }
  }
}
