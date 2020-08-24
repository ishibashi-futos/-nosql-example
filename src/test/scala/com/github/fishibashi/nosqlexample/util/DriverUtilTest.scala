package com.github.fishibashi.nosqlexample.util

import java.sql.{Connection, PreparedStatement}

import org.junit.runner.RunWith
import org.scalatest.funsuite.AnyFunSuite
import org.scalatestplus.junit.JUnitRunner

@RunWith(classOf[JUnitRunner])
class DriverUtilTest extends AnyFunSuite {
  test("connectionがnullの状態で、connectionをCloseしてもエラーにならない") {
    val conn: Connection = null
    DriverUtil.closeConnection(conn)
  }

  test("connectionがopenの状態で、connectionをCloseする") {
    val conn = ConnectionManager.newSqlConnection()
    assert(!conn.isClosed)
    DriverUtil.closeConnection(conn)
    assert(conn.isClosed)
  }

  test("preparedStatementがnullの状態で、closeしてもエラーにならない") {
    val stmt: PreparedStatement = null
    DriverUtil.closeStatement(stmt)
  }

  test("statementがopenの状態で、stmtがcloseできる") {
    val stmt = ConnectionManager.newSqlConnection().prepareStatement("")
    DriverUtil.closeStatement(stmt)
    assert(stmt.isClosed)
  }
}
