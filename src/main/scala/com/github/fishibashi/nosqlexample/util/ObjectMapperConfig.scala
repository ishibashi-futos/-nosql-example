package com.github.fishibashi.nosqlexample.util

import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.module.scala.{DefaultScalaModule, ScalaObjectMapper}

sealed trait ObjectMapperConfig {
  def getObjectMapper: ObjectMapper
}

object DefaultMapperConfig extends ObjectMapperConfig {
  private val mapper = (new ObjectMapper() with ScalaObjectMapper).registerModule(DefaultScalaModule)

  override def getObjectMapper: ObjectMapper = mapper
}
