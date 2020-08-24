package com.github.fishibashi.nosqlexample.db

import com.github.fishibashi.nosqlexample.util.ConnectionManager
import com.google.gson.Gson
import com.mongodb.client.MongoDatabase
import org.bson.Document

trait MongoRepositoryTest[T] {
  var conn: MongoDatabase = _
  var closer: () => Unit = _

  protected def init(): Unit = {
    val (conn, closer) = ConnectionManager.newMongoDBConnection()
    this.conn = conn
    this.closer = closer
  }

  protected def setUp(colName: String, data: Array[T]): Unit = {
    val col = conn.getCollection(colName)
    data.foreach(v => {
      val gson = new Gson()
      val document = Document.parse(gson.toJson(v))
      col.insertOne(document)
    })
  }

  protected def tearDown(): Unit = {
    closer()
  }

}
