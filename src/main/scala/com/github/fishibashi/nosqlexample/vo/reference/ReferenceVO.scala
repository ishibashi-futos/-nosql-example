package com.github.fishibashi.nosqlexample.vo.reference

case class ReferenceVO(taskId: String,
                       startTime: String,
                       endTime: String,
                       totalTime: Int,
                       clientIp: String,
                       referenceTable: Map[Int, PointReferenceVO])
