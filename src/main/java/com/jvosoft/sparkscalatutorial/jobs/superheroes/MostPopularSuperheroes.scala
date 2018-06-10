package com.jvosoft.sparkscalatutorial.jobs.superheroes

import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkContext
import org.springframework.beans.factory.annotation.Value
import org.springframework.scheduling.annotation.Scheduled
import org.springframework.stereotype.Component

@Component
class MostPopularSuperheroes(@Value("${jobs.superheroes.most-popular-hero.active}") active: Boolean,
                             @Value("${jobs.superheroes.most-popular-hero.main-data-path}") mainFilePath: String,
                             @Value("${jobs.superheroes.most-popular-hero.additional-data-path}") additionalFilePath: String) extends Serializable {

  @Scheduled(fixedDelay = 50000)
  def execute(): Unit = {
    if (!active) {
      return
    }
    Logger.getLogger("org").setLevel(Level.ERROR)
    println("Job executed: MostPopularSuperheroes")

    // Create a SparkContext using every core of the local machine
    val sc = new SparkContext("local[*]", "MostPopularSuperhero")

    // Build up a hero ID -> name RDD
    val names = sc.textFile(additionalFilePath)
    val namesRdd = names.flatMap(parseNames)

    // Load up the superhero co-apperarance data
    val lines = sc.textFile("datasources/Marvel-graph.txt")

    // Convert to (heroID, number of connections) RDD
    val pairings = lines.map(countCoOccurences)

    // Combine entries that span more than one line
    val totalFriendsByCharacter = pairings.reduceByKey((x, y) => x + y)

    // Flip it to # of connections, hero ID
    val flipped = totalFriendsByCharacter.map(x => (x._2, x._1))

    // Find the max # of connections
    val mostPopular = flipped.max()

    // Look up the name (lookup returns an array of results, so we need to access the first result with (0)).
    val mostPopularName = namesRdd.lookup(mostPopular._2).head

    // Print out our answer!
    println(s"$mostPopularName is the most popular superhero with ${mostPopular._1} co-appearances.")
  }

  // Function to extract the hero ID and number of connections from each line
  def countCoOccurences(line: String): (Int, Int) = {
    var elements = line.split("\\s+")
    (elements(0).toInt, elements.length - 1)
  }

  // Function to extract hero ID -> hero name tuples (or None in case of failure)
  def parseNames(line: String): Option[(Int, String)] = {
    val fields = line.split('\"')
    if (fields.length > 1) {
      Some(fields(0).trim().toInt, fields(1))
    } else {
      None // flatmap will just discard None results, and extract data from Some results.
    }
  }
}
