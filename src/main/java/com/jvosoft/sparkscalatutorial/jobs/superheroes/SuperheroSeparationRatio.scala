//package com.jvosoft.sparkscalatutorial.jobs.superheroes
//
//import org.apache.log4j.{Level, Logger}
//import org.springframework.beans.factory.annotation.Value
//import org.springframework.scheduling.annotation.Scheduled
//import org.springframework.stereotype.Component
//
//@Component
//class SuperheroSeparationRatio(@Value("${jobs.superheroes.most-popular-hero.active}") active: Boolean,
//                               @Value("${jobs.superheroes.most-popular-hero.main-data-path}") mainFilePath: String,
//                               @Value("${jobs.superheroes.most-popular-hero.additional-data-path}") additionalFilePath: String) extends Serializable {
//
//  @Scheduled(fixedDelay = 50000)
//  def execute(): Unit = {
//    if (!active) {
//      return
//    }
//    Logger.getLogger("org").setLevel(Level.ERROR)
//    println("Job executed: SuperheroSeparationRatio")
//
//  }
//}
