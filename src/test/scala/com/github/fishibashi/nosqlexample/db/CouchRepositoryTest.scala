package com.github.fishibashi.nosqlexample.db

import com.couchbase.client.java.Bucket
import com.couchbase.client.java.json.JsonObject
import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.module.scala.DefaultScalaModule
import com.github.fishibashi.nosqlexample.util.{ConnectionManager, DefaultMapperConfig}
import org.slf4j.LoggerFactory

trait CouchRepositoryTest[K, V] {
  private val logger = LoggerFactory.getLogger("CouchRepositoryTest")
  private var conn: Bucket = _
  private var closer: () => Unit = _
  private var mapper: ObjectMapper = _

  def init(): Unit = {
    val (conn, closer) = ConnectionManager.newCouchConnection()
    this.conn = conn
    this.closer = closer
    this.mapper = DefaultMapperConfig.getObjectMapper()
  }

  def setUp(collectionName: String, items: Array[(K, V)]): Unit = {
    val col = this.conn.defaultCollection()
    items.foreach(item => {
      val json = mapper.writeValueAsString(item._2)
      logger.info(s"${item._1}, json={${json}}")
      col.insert(item._1.toString, JsonObject.fromJson(json))
    })
  }


  def tearDown(): Unit = {
    this.closer()
  }
}
