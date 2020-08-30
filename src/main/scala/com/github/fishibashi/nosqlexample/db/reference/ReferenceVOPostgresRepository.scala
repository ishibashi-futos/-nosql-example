package com.github.fishibashi.nosqlexample.db.reference

import java.sql.{Connection, PreparedStatement}

import com.github.fishibashi.nosqlexample.Global.using
import com.github.fishibashi.nosqlexample.util.DriverUtil
import com.github.fishibashi.nosqlexample.vo.reference._
import org.slf4j.LoggerFactory

import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.duration._
import scala.concurrent.{Await, Future}
import scala.util.{Failure, Success, Try}

class ReferenceVOPostgresRepository(val conn: Connection) extends ReferenceVORepository {

  private val logger = LoggerFactory.getLogger(classOf[ReferenceVOPostgresRepository])
  private val executeQuery = (sql: String, key: String) => using[PreparedStatement, Unit](conn.prepareStatement(sql)) { stmt =>
    stmt.setString(1, key)
    stmt.executeUpdate()
  }

  override def save(data: ReferenceVO): Try[Int] = {
    try {
      saveReferenceVO(data)
      data.referenceTable.foreach((item) => savePointRefVO(item))
      Success(1)
    } catch {
      case e: Throwable => Failure(e)
    }
  }

  protected def saveReferenceVO(referenceVo: ReferenceVO): Unit = {
    val sql = "INSERT INTO ReferenceVO VALUES(?,?,?,?,?,?)"
    var stmt: PreparedStatement = null
    try {
      stmt = conn.prepareStatement(sql)
      stmt.setString(1, referenceVo.taskId)
      stmt.setString(2, referenceVo.systemId)
      stmt.setString(3, referenceVo.startTime)
      stmt.setString(4, referenceVo.endTime)
      stmt.setLong(5, referenceVo.totalTime)
      stmt.setString(6, referenceVo.clientIp)
      stmt.executeUpdate()
    } finally {
      DriverUtil.closeStatement(stmt)
    }
  }

  protected def savePointRefVO(pointRefVo: PointReferenceVO): Unit = {
    val sql = "INSERT INTO PointReferenceVO VALUES(?,?,?,?,?,?,?,?,?,?,?)"
    var stmt: PreparedStatement = null
    try {
      stmt = conn.prepareStatement(sql)
      stmt.setString(1, pointRefVo.taskId)
      stmt.setString(2, pointRefVo.referenceId)
      stmt.setString(3, pointRefVo.parentReferenceId)
      stmt.setLong(4, pointRefVo.pointCount)
      stmt.setString(5, pointRefVo.startTime)
      stmt.setString(6, pointRefVo.endTime)
      stmt.setLong(7, pointRefVo.totalTime)
      stmt.setString(8, pointRefVo.className)
      stmt.setString(9, pointRefVo.methodName)
      stmt.setBytes(10, PointReferenceVO.dataTableToByteArray(pointRefVo.dataTable))
      stmt.setString(11, pointRefVo.isSuccess)
      stmt.executeUpdate()
    } finally {
      DriverUtil.closeStatement(stmt)
    }
  }

  override def findOne(key: String): Option[ReferenceVO] = {
    val findRefVo = findOneRefVo(key)
    val findPointRefVo = findByTaskIdPointRefVo(key)
    val async = for {
      refVo <- findRefVo
      pointRefVo <- findPointRefVo
    } yield {
      refVo match {
        case Some(referenceVO) => Some(referenceVO.copy(referenceTable = pointRefVo.getOrElse(List())))
        case _ => None
      }
    }
    Await.result(async, Duration.Inf)
  }

  private def findOneRefVo(key: String): Future[Option[ReferenceVO]] = Future {
    val sql = "SELECT * FROM REFERENCEVO WHERE TASKID = ?"
    using[PreparedStatement, ReferenceVO](conn.prepareStatement(sql)) { stmt =>
      stmt.setString(1, key)
      val rs = stmt.executeQuery()
      if (!rs.next()) throw new Exception(s"Not Found taskId=${key}")
      ReferenceVO(rs.getString("TASKID"),
        rs.getString("SYSTEMID"),
        rs.getString("STARTTIME"),
        rs.getString("ENDTIME"),
        rs.getLong("TOTALTIME"),
        rs.getString("CLIENTIP"),
        List()
      )
    } match {
      case Success(value) => Some(value)
      case Failure(_) => None
    }
  }

  private def findByTaskIdPointRefVo(key: String): Future[Option[List[PointReferenceVO]]] = Future {
    val sql = "SELECT * FROM POINTREFERENCEVO WHERE TASKID = ? ORDER BY POINTCOUNT"
    using(conn.prepareStatement(sql)) { stmt => {
      stmt.setString(1, key)
      val rs = stmt.executeQuery()
      val array = scala.collection.mutable.ArrayBuffer[PointReferenceVO]()
      var i = 0
      if (rs.next()) {
        array.append(PointReferenceVO(
          rs.getString("TASKID"),
          rs.getString("REFERENCEID"),
          rs.getString("PARENTREFERENCEID"),
          rs.getLong("POINTCOUNT"),
          rs.getString("STARTTIME"),
          rs.getString("ENDTIME"),
          rs.getLong("TOTALTIME"),
          rs.getString("CLASSNAME"),
          rs.getString("METHODNAME"),
          PointReferenceVO.byteArrayToDataTable(rs.getBytes("DATATABLE")),
          rs.getString("ISSUCCESS")
        ))
        i = i + 1
      }
      Success(array)
    }
    } match {
      case Success(value) => Some(value.get.toList)
      case Failure(_) => None
    }
  }

  override def delete(key: String): Unit = {
    val pointReferenceVO = Await.result(findOneRefVo(key), Duration.Inf)
    pointReferenceVO match {
      case None => logger.info(s"was not deleted. taskId = ${key}")
      case Some(_) =>
        val complete = Future.sequence(Seq(deleteRefVo(key), deletePointRefVo(key)))
        Await.result(complete, Duration.Inf)
    }
  }

  private def deleteRefVo(key: String): Future[Try[Unit]] = Future {
    val sql = "DELETE FROM REFERENCEVO WHERE TASKID = ?"
    executeQuery(sql, key)
  }

  private def deletePointRefVo(key: String): Future[Try[Unit]] = Future {
    val sql = "DELETE FROM POINTREFERENCEVO WHERE TASKID = ?"
    executeQuery(sql, key)
  }
}
