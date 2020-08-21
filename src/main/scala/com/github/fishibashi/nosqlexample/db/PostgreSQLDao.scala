package com.github.fishibashi.nosqlexample.db

import java.sql.{Connection, PreparedStatement}

import com.github.fishibashi.nosqlexample.vo.{DiagnosPointRefVo, DiagnosReferenceVo, MonitoringData}

import scala.util.{Failure, Success, Try}

class PostgreSQLDao(val conn: Connection) extends Dao {
  override def save(data: MonitoringData): Unit = {
    val refVo = saveRefVo(data.refVo)
    refVo.recover({
      case e: Throwable => {
        conn.rollback()
        throw e
      }
    })
  }

  private def saveRefVo(vo: DiagnosReferenceVo): Try[Int] = {
    val sql = "INSERT INTO DIAGNOSREFERENCEVO (taskId, startTime, endTime, agentId, totalTime, systemId, functionId, screenId, actionId) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)"
    var stmt: PreparedStatement = null
    try {
      stmt = conn.prepareStatement(sql)
      stmt.setString(1, vo.taskId)
      stmt.setString(2, vo.startTime)
      stmt.setString(3, vo.endTime)
      stmt.setString(4, vo.agentId)
      stmt.setLong(5, vo.totalTime.toLong)
      stmt.setString(6, vo.systemId)
      stmt.setString(7, vo.functionId)
      stmt.setString(8, vo.screenId)
      stmt.setString(9, vo.actionId)
      Success(stmt.executeUpdate())
    } catch {
      case e: Throwable => Failure(e)
    } finally {
      stmt.close()
    }
  }

  private def savePointVo(vo: DiagnosPointRefVo): Try[Int] = {
    val sql = "INSERT INTO DIAGNOSPOINTREFVO (REFERENCEID, PARENTREFERENCEID, POINTCOUNT, CLASSNAME, METHODNAME, DATATABLE, TASKID, STARTTIME, ENDTIME, ISSUCCESS, TOTALTIME) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)"
    var stmt: PreparedStatement = null
    try {
      stmt = conn.prepareStatement(sql)
    } catch {
      case e: Throwable => Failure(e)
    } finally {
      stmt.close()
    }
    Success(1)
  }

  override def saveAll(data: Seq[MonitoringData]): Unit = ???

  override def findByTaskId(): MonitoringData = ???

  override def findByStartTime(startTime: String, limit: Int, offset: Int): Unit = ???

  override def delete(taskId: String): Unit = ???

  override def deleteFromRange(startTime: String, endTime: String): Unit = ???

  override def update(data: MonitoringData): Unit = ???
}
