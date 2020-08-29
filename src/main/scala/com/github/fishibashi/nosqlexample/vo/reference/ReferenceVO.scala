package com.github.fishibashi.nosqlexample.vo.reference

import com.github.fishibashi.nosqlexample.util.DefaultMapperConfig
import org.slf4j.LoggerFactory

import collection.JavaConverters._

case class ReferenceVO(taskId: String,
                       systemId: String,
                       startTime: String,
                       endTime: String,
                       totalTime: Long,
                       clientIp: String,
                       referenceTable: Map[Int, PointReferenceVO]) {
  private val logger = LoggerFactory.getLogger(classOf[ReferenceVO])
  def toJson: String = DefaultMapperConfig.getObjectMapper.writeValueAsString(this)
}
