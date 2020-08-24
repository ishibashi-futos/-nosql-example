package com.github.fishibashi.nosqlexample.db

import java.sql.Connection

import com.github.fishibashi.nosqlexample.util.{ConnectionManager, JDBCUtil}

trait SQLRepositoryTest {
  var conn: Connection = _

  protected def init(): Unit = {
    conn = ConnectionManager.newSqlConnection()
  }

  protected def tearDown(): Unit = {
    JDBCUtil.closeConnection(conn)
  }

  protected def setup(setupScripts: Array[String]): Unit = {
    setupScripts.foreach(sql => {
      val stmt = conn.prepareStatement(sql)
      stmt.executeUpdate()
      conn.commit()
      JDBCUtil.closeStatement(stmt)
    })
  }
}
