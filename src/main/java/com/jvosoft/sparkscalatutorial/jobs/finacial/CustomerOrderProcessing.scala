package com.jvosoft.sparkscalatutorial.jobs.finacial

import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkContext
import org.springframework.beans.factory.annotation.Value
import org.springframework.scheduling.annotation.Scheduled
import org.springframework.stereotype.Component

@Component
class CustomerOrderProcessing(@Value("${jobs.financial.order-processing.active}") active: Boolean,
                              @Value("${jobs.financial.order-processing.path}") filePath: String) extends Serializable {

  @Scheduled(cron = "${jobs.default-schedule}")
  def execute(): Unit = {
    if (!active) {
      return
    }
    Logger.getLogger("org").setLevel(Level.ERROR)
    println("Job executed: FriendsByAge")

    val sparkContext = new SparkContext("local[*]", "spark-tutorial")

    val result = sparkContext.textFile(filePath)
      .map(extract)
      .reduceByKey(_ + _)
      .sortBy(_._2, ascending = false)
      .collect()

    result.foreach(println)

  }

  def extract(line: String): (Int, Float) = {
    val fields = line.split(",")
    val customerId = fields(0).toInt
    val sum = fields(2).toFloat
    (customerId, sum)
  }

}
