package com.github.fishibashi.nosqlexample.vo.reference

import com.fasterxml.jackson.annotation.JsonIgnoreProperties
import com.github.fishibashi.nosqlexample.util.DefaultMapperConfig
import org.slf4j.LoggerFactory

@JsonIgnoreProperties(ignoreUnknown = true)
case class ReferenceVO(taskId: String,
                       systemId: String,
                       startTime: String,
                       endTime: String,
                       totalTime: Long,
                       clientIp: String,
                       referenceTable: List[PointReferenceVO]) {
  private val logger = LoggerFactory.getLogger(classOf[ReferenceVO])

  def toJson: String = DefaultMapperConfig.getObjectMapper.writeValueAsString(this)
}
