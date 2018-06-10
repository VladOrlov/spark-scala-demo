package com.jvosoft.sparkscalatutorial.demo

import org.json4s.reflect.Reflector

import scala.beans.BeanProperty

class Person {
  @BeanProperty var name: String = "Vasya Pupkin"
  @BeanProperty var age: Int = 25
}

object Person extends App {

}
