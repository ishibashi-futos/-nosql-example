package com.github.fishibashi.nosqlexample.util

import java.sql.{Connection, DriverManager}

import com.couchbase.client.java.{Bucket, Cluster}
import com.mongodb.client._

object ConnectionManager {
  private val wsl2Address = "172.30.240.230"
  private val url = s"jdbc:postgresql://${wsl2Address}:5432/postgres"
  private val user = "postgres"
  private val password = "postgres"

  private val mongodbName = "testdb"
  private val mongoUser = "mongos"
  private val mongoPasswd = "mongos"
  private val mongoPort = 27017
  private val mongoUrl = s"mongodb://${mongoUser}:${mongoPasswd}@${wsl2Address}:${mongoPort}"

  private val couchUser = "couchbase"
  private val couchPasswd = "couchbase"
  private val couchBucketName = "testdb"

  def newSqlConnection(): Connection = {
    val conn = DriverManager.getConnection(url, user, password)
    conn.setAutoCommit(false)
    conn
  }

  def newMongoDBConnection(): (MongoDatabase, Function0[Unit]) = {
    val client = MongoClients.create(mongoUrl)
    val database = client.getDatabase(mongodbName)
    (database, () => {
      DriverUtil.closeMongoConnection(client)
    })
  }

  def newCouchConnection(): (Bucket, () => Unit) = {
    val cluster = Cluster.connect(s"${wsl2Address}", couchUser, couchPasswd)
    (cluster.bucket(couchBucketName), () => {
      cluster.disconnect()
    })
  }
}
