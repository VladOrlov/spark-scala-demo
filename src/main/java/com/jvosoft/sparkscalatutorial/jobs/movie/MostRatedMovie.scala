package com.jvosoft.sparkscalatutorial.jobs.movie

import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkContext
import org.springframework.beans.factory.annotation.Value
import org.springframework.scheduling.annotation.Scheduled
import org.springframework.stereotype.Component

@Component
class MostRatedMovie(@Value("${jobs.movie.most-rated-movie.active}") active: Boolean,
                     @Value("${jobs.movie.most-rated-movie.path}") filePath: String) extends Serializable {

  @Scheduled(cron = "${jobs.default-schedule}")
  def execute(): Unit = {
    if (!active) {
      return
    }
    Logger.getLogger("org").setLevel(Level.ERROR)
    println("Job executed: FriendsByAge")

    val sparkContext = new SparkContext("local[*]", "spark-tutorial")
    val linesRdd = sparkContext.textFile("datasources/movies/ratings.csv")

    val result = linesRdd.map(extract)
      .reduceByKey((tupleA, tupleB) => (tupleA._1 + tupleB._1, tupleA._2 + tupleB._2))
      .mapValues(tuple => tuple._1 / tuple._2)
      .sortBy(_._2, ascending = false)
      .collect()

    result.foreach(println)
  }

  private def extract(line: String): (Int, (Float, Int)) = {
    val fields = line.split(",")
    val movieId = fields(1).toInt
    val rating = fields(2).toFloat
    (movieId, (rating, 1))
  }
}
