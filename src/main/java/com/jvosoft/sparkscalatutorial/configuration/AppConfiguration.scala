//package com.jvosoft.sparkscalatutorial.configuration
//
//import org.apache.spark.SparkContext
//import org.apache.spark.sql.SparkSession
//import org.springframework.boot.SpringBootConfiguration
//import org.springframework.context.annotation.Bean
//
//@SpringBootConfiguration
//class AppConfiguration {
//
//  @Bean
//  def sparkContext(): SparkContext = {
//    new SparkContext("local[*]", "spark-tutorial")
//  }
//
//    @Bean
//    def sparkSession(): SparkSession = {
//      SparkSession.builder()
//        .appName("spark-tutorial")
//        .master("local[*]")
//        .getOrCreate()
//    }
//}
