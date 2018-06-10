package com.jvosoft.sparkscalatutorial.jobs.friends

import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkContext
import org.apache.spark.sql.SparkSession
import org.springframework.beans.factory.annotation.{Autowired, Value}
import org.springframework.scheduling.annotation.Scheduled
import org.springframework.stereotype.Component

@Component
class FriendsByAge(@Autowired(required = false) sparkSession: SparkSession,
                   @Value("${jobs.friends.active}") active: Boolean) extends Serializable {

  @Scheduled(cron = "${jobs.default-schedule}")
  def execute(): Unit = {
    if(!active){
      return
    }
    Logger.getLogger("org").setLevel(Level.ERROR)
    println("Job executed: FriendsByAge")

    val sparkContext = new SparkContext("local[*]", "spark-tutorial")
    val linesRdd = sparkContext.textFile("datasources/fakefriends.csv")

    // Use our parseLines function to convert to (age, numFriends) tuples
    val friendsByAge = linesRdd.map(parseLine).cache()

    val totalsByAge = friendsByAge.mapValues(x => (x, 1))
      .reduceByKey((x, y) => (x._1 + y._1, x._2 + y._2))

    val totalByAgeResult = totalsByAge.collect()
    totalByAgeResult.sorted.foreach(println)

    // So now we have tuples of (age, (totalFriends, totalInstances))
    // To compute the average we divide totalFriends / totalInstances for each age.
    val averagesByAge = totalsByAge.mapValues(x => x._1 / x._2)

    // Collect the avgAgeResult from the RDD (This kicks off computing the DAG and actually executes the job)
    val avgAgeResult = averagesByAge.collect()
    avgAgeResult.sorted.foreach(println)

  }

  def parseLine(line: String): (Int, Int) = {
    val fields = line.split(",")
    val age = fields(2).toInt
    val numFriends = fields(3).toInt
    (age, numFriends)
  }
}
