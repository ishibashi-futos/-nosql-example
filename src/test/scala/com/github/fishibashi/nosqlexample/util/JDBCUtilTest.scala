package com.github.fishibashi.nosqlexample.util

import java.sql.{Connection, PreparedStatement}

import org.junit.runner.RunWith
import org.scalatest.funsuite.AnyFunSuite
import org.scalatestplus.junit.JUnitRunner

@RunWith(classOf[JUnitRunner])
class JDBCUtilTest extends AnyFunSuite {
  test("connectionがnullの状態で、connectionをCloseしてもエラーにならない") {
    val conn: Connection = null
    JDBCUtil.closeConnection(conn)
  }

  test("connectionがopenの状態で、connectionをCloseする") {
    val conn = ConnectionManager.newSqlConnection()
    assert(!conn.isClosed)
    JDBCUtil.closeConnection(conn)
    assert(conn.isClosed)
  }

  test("preparedStatementがnullの状態で、closeしてもエラーにならない") {
    val stmt: PreparedStatement = null
    JDBCUtil.closeStatement(stmt)
  }

  test("statementがopenの状態で、stmtがcloseできる") {
    val stmt = ConnectionManager.newSqlConnection().prepareStatement("")
    JDBCUtil.closeStatement(stmt)
    assert(stmt.isClosed)
  }
}
