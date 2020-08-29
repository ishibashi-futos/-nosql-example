package com.github.fishibashi.nosqlexample.util

import java.sql.{Connection, DriverManager}

import com.couchbase.client.java.{Bucket, Cluster}
import com.mongodb.{MongoClientSettings, ServerAddress}
import com.mongodb.client._

object ConnectionManager {
  private val url = s"jdbc:postgresql://wsl:5432/postgres"
  private val user = "postgres"
  private val password = "postgres"

  private val mongodbName = "testdb"
  private val mongoUser = "mongos"
  private val mongoPasswd = "mongos"
  // mongoのtransaction機能を使うためにはReplicaSetを構築する必要がある
  // また、replicaSetを構築した場合、クライアントマシンから各ReplicaSetに対してホスト名（initiate.membersに指定したhostの名称部分）を
  // が解決できる必要があるため、hostsファイルなどによる名称解決ができるようにしておく必要がある
  // 各replicaSetのアドレスをipで指定したとしても、hostの名称部分で解決しようとするため注意（特にDockerで使うとき）
  private val mongoUrl = s"mongodb://${mongoUser}:${mongoPasswd}@mongo-primary:27017,mongo-secondary:27018/?replicaSet=replset"

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

  def newMongoClient: MongoClient = MongoClients.create(mongoUrl)

  def newCouchConnection(): (Bucket, () => Unit) = {
    val cluster = Cluster.connect("wsl", couchUser, couchPasswd)
    (cluster.bucket(couchBucketName), () => {
      cluster.disconnect()
    })
  }
}
