package com.github.fishibashi.nosqlexample.util

import java.sql.{Connection, PreparedStatement}

object DriverUtil {
  def closeStatement(stmt: PreparedStatement): Unit = {
    Option(stmt) match {
      case Some(stmt) => stmt.close()
      case None => // not doing anything.
    }
  }

  def closeConnection(conn: Connection): Unit = {
    Option(conn) match {
      case Some(conn) => conn.close()
      case None => // not doing anything.
    }
  }
}
