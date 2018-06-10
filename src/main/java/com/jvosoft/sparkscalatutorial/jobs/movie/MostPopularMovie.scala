package com.jvosoft.sparkscalatutorial.jobs.movie

import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkContext
import org.springframework.beans.factory.annotation.{Autowired, Value}
import org.springframework.scheduling.annotation.Scheduled
import org.springframework.stereotype.Component

@Component
class MostPopularMovie(@Autowired movieHelper: MovieHelper,
                       @Value("${jobs.movie.most-popular-movie.active}") active: Boolean,
                       @Value("${jobs.movie.most-popular-movie.path}") filePath: String) extends Serializable {

  @Scheduled(cron = "${jobs.default-schedule}")
  def execute(): Unit = {
    if (!active) {
      return
    }
    Logger.getLogger("org").setLevel(Level.ERROR)
    println("Job executed: most popular movies")

    val sparkContext = new SparkContext("local[*]", "spark-tutorial")
    val nameDict = sparkContext.broadcast(movieHelper.getMovieNames())
    val linesRdd = sparkContext.textFile("datasources/movies/ratings.csv")

    val result = linesRdd
      .map(x => (x.split(",")(1).toInt, 1))
      .reduceByKey(_+_)
      .map(tuple => tuple.swap)
      .sortByKey(ascending = false)
      .map(tuple  => (nameDict.value(tuple._2), tuple._1))
      .take(20)

    result.foreach(println)
  }

  private def extract(line: String): (Int, (Float, Int)) = {
    val fields = line.split(",")
    val movieId = fields(1).toInt
    val rating = fields(2).toFloat
    (movieId, (rating, 1))
  }
}
