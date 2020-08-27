package com.github.fishibashi.nosqlexample.vo.reference

import java.io._

import com.github.fishibashi.nosqlexample.vo.reference.PointReferenceVO.dataTableToByteArray

case class PointReferenceVO(taskId: String,
                            referenceId: String,
                            parentReferenceId: String,
                            pointCount: Long,
                            startTime: String,
                            endTime: String,
                            totalTime: Long,
                            className: String,
                            methodName: String,
                            dataTable: Map[String, AnyRef],
                            isSuccess: String) {
  def getDataTableToByte: Array[Byte] = dataTableToByteArray(dataTable)

}

object PointReferenceVO {
  def dataTableToByteArray(dataTable: Map[String, AnyRef]): Array[Byte] = {
    val byteArrayOutputStream = new ByteArrayOutputStream()
    var objectOutputStream: ObjectOutputStream = null
    try {
      objectOutputStream = new ObjectOutputStream(byteArrayOutputStream)
      objectOutputStream.writeObject(dataTable)
      objectOutputStream.flush()
      byteArrayOutputStream.toByteArray
    } finally {
      byteArrayOutputStream.close()
    }
  }

  def byteArrayToDataTable(bytes: Array[Byte]): Map[String, AnyRef] = {
    val stream = new ByteArrayInputStream(bytes)
    var input: ObjectInput = null
    try {
      input = new ObjectInputStream(stream)
      input.readObject().asInstanceOf[Map[String, AnyRef]]
    } finally {
      input match {
        case null =>
        case _ => input.close()
      }
    }
  }
}