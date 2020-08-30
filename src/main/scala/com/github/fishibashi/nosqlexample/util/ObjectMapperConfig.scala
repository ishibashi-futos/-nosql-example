package com.github.fishibashi.nosqlexample.util

import com.fasterxml.jackson.databind.{DeserializationContext, KeyDeserializer, ObjectMapper}
import com.fasterxml.jackson.databind.module.SimpleModule
import com.fasterxml.jackson.module.scala.{DefaultScalaModule, ScalaObjectMapper}

sealed trait ObjectMapperConfig {
  def getObjectMapper: ObjectMapper
}

object DefaultMapperConfig extends ObjectMapperConfig {
  private val module = new SimpleModule().addKeyDeserializer(classOf[Integer], (key: String, ctxt: DeserializationContext) => {
    Integer.parseInt(key)
  })
  private lazy val mapper = (new ObjectMapper() with ScalaObjectMapper).registerModule(DefaultScalaModule).registerModule(module)

  override def getObjectMapper: ObjectMapper = {
    this.mapper
  }
}
