package com.jvosoft.sparkscalatutorial.jobs.weather

import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkContext
import org.springframework.beans.factory.annotation.Value
import org.springframework.scheduling.annotation.Scheduled
import org.springframework.stereotype.Component

@Component
class SimpleWordCount(@Value("${jobs.wordcount.simple-wordcount.active}") active: Boolean,
                      @Value("${jobs.wordcount.simple-wordcount.path}") filePath: String) extends Serializable {

  @Scheduled(cron = "${jobs.default-schedule}")
  def execute(): Unit = {
    if (!active) {
      return
    }
    Logger.getLogger("org").setLevel(Level.ERROR)

    val sc = new SparkContext("local[*]", "WordCount")

    val wordCounts = sc.textFile(filePath)
      .flatMap(line => line.split("\\W+"))
      .map(word => (word.toLowerCase(), 1))
      .reduceByKey(_ + _)
      .sortBy(_._2, ascending = false)
      .collect()

    wordCounts.foreach(println)
  }
}
