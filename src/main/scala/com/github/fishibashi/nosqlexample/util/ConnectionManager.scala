package com.github.fishibashi.nosqlexample.util

import java.sql.{Connection, DriverManager}
import com.mongodb.client._

object ConnectionManager {
  private val wsl2Address = "172.30.240.230"
  private val url = s"jdbc:postgresql://${wsl2Address}:5432/postgres"
  private val user = "postgres"
  private val password = "postgres"

  private val mongodbName = "testdb"

  def newSqlConnection(): Connection = {
    val conn = DriverManager.getConnection(url, user, password)
    conn.setAutoCommit(false)
    conn
  }

  def newMongoDBConnection(): (MongoDatabase, Function0[Unit]) = {
    val client = MongoClients.create(s"mongodb://${wsl2Address}:2717")
    (client.getDatabase(mongodbName), () => {
      DriverUtil.closeMongoConnection(client)
    })
  }
}
