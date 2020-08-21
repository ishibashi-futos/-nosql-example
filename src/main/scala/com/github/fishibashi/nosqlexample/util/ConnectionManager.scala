package com.github.fishibashi.nosqlexample.util

import java.sql.{Connection, DriverManager}

object ConnectionManager {
  private val wsl2Address = "172.30.252.151"
  private val url = s"jdbc:postgresql://${wsl2Address}:5432/postgres"
  private val user = "postgres"
  private val password = "postgres"

  def newSqlConnection(): Connection = {
    val conn = DriverManager.getConnection(url, user, password)
    conn.setAutoCommit(false)
    conn
  }
}
