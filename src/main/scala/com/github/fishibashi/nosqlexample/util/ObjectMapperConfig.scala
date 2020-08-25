package com.github.fishibashi.nosqlexample.util

import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.module.scala.DefaultScalaModule

sealed trait ObjectMapperConfig {
  def getObjectMapper: ObjectMapper
}

object DefaultMapperConfig extends ObjectMapperConfig {
  override def getObjectMapper: ObjectMapper =
    new ObjectMapper().registerModule(DefaultScalaModule)
}
