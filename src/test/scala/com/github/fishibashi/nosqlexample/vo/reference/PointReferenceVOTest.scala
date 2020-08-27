package com.github.fishibashi.nosqlexample.vo.reference

import java.time.LocalDateTime
import java.time.format.DateTimeFormatter
import java.util.UUID

import com.github.fishibashi.nosqlexample.util.DefaultMapperConfig
import org.junit.runner.RunWith
import org.scalatest.funsuite.AnyFunSuite
import org.scalatestplus.junit.JUnitRunner

import scala.collection.immutable.Map

@RunWith(classOf[JUnitRunner])
class PointReferenceVOTest extends AnyFunSuite {
  private val formatter = DateTimeFormatter.ofPattern("yyyyMMddHHmmssSSS")

  test("DataTableをMapでコンストラクタ引数に取得できる") {
    val dataTable = Map("a" -> "String", "b" -> 100, "c" -> 100L)
    val pointRefVo = PointReferenceVO(
      UUID.randomUUID().toString,
      UUID.randomUUID().toString,
      UUID.randomUUID().toString,
      Math.random().toLong,
      formatter.format(LocalDateTime.now()),
      formatter.format(LocalDateTime.now()),
      Math.random().toLong,
      this.getClass.toString,
      "test",
      dataTable.asInstanceOf[Map[String, AnyRef]],
      "1"
    )

    assert(pointRefVo.className == this.getClass.toString)
    assert(pointRefVo.dataTable.get("a").get == "String")
    assert(pointRefVo.dataTable.get("b").get == 100)
    assert(pointRefVo.dataTable.get("c").get == 100L)
  }

  test("MapをByte配列に変換し、元のMapに変換できる") {
    val dataTable = Map("a" -> "String", "b" -> 100, "c" -> 100L)
    val bytes = PointReferenceVO.dataTableToByteArray(dataTable.asInstanceOf[Map[String, AnyRef]])
    val parsed = PointReferenceVO.byteArrayToDataTable(bytes)
    dataTable.keys.foreach(key => {
      assert(dataTable.get(key).get == parsed.get(key).get)
    })
  }
}
