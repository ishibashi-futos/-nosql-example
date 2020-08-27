package com.github.fishibashi.nosqlexample.vo.reference

case class ReferenceVO(taskId: String,
                       systemId: String,
                       startTime: String,
                       endTime: String,
                       totalTime: Long,
                       clientIp: String,
                       referenceTable: Map[Int, PointReferenceVO])
