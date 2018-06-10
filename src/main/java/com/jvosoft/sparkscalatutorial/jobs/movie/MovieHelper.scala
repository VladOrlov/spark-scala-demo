package com.jvosoft.sparkscalatutorial.jobs.movie

import java.nio.charset.CodingErrorAction

import org.springframework.stereotype.Component

import scala.io.{Codec, Source}

@Component
class MovieHelper {

  def getMovieNames() : Map[Int, String] = {

    // Handle character encoding issues:
    implicit val codec = Codec("UTF-8")
    codec.onMalformedInput(CodingErrorAction.REPLACE)
    codec.onUnmappableCharacter(CodingErrorAction.REPLACE)

    // Create a Map of Ints to Strings, and populate it from u.item.
    var movieNames:Map[Int, String] = Map()

    val lines = Source.fromFile("datasources/movies/movies.csv").getLines()
    for (line <- lines) {
      val fields = line.split(',')
      if (fields.length > 1) {
        movieNames += (fields(0).toInt -> fields(1))
      }
    }

    return movieNames
  }

}
