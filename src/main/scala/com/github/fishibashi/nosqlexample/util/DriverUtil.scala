package com.github.fishibashi.nosqlexample.util

import java.sql.{Connection, PreparedStatement}

import com.mongodb.client.MongoClient
import org.slf4j.LoggerFactory

class DriverUtil

object DriverUtil {
  private val logger = LoggerFactory.getLogger(classOf[DriverUtil])
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

  def closeMongoConnection(conn: MongoClient): Unit = {
    logger.info("Begin closing the mongo connection.")
    Option(conn) match {
      case Some(conn) => conn.close()
      case None => logger.info("client is null.")
    }
    logger.info("End closed the mongo connection.")
  }
}
