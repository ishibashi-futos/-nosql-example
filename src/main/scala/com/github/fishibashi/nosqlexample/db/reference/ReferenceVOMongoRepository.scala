package com.github.fishibashi.nosqlexample.db.reference

import com.fasterxml.jackson.databind.DeserializationFeature
import com.github.fishibashi.nosqlexample.util.DefaultMapperConfig
import com.github.fishibashi.nosqlexample.vo.reference.ReferenceVO
import com.mongodb.ClientSessionOptions
import com.mongodb.client.model.Filters
import com.mongodb.client.{ClientSession, MongoClient, MongoCollection}
import org.bson.Document

import scala.util.{Failure, Success, Try}

class ReferenceVOMongoRepository(val client: MongoClient) extends ReferenceVORepository {

  private var session: ClientSession = _
  private val mapper = DefaultMapperConfig.getObjectMapper

  def setTransaction(): ClientSession = {
    this.session = client.startSession(ClientSessionOptions.builder().causallyConsistent(true).build())
    session
  }

  override def save(data: ReferenceVO): Try[Int] = {
    startedTransaction()
    val collection = getCollection
    try {
      session.startTransaction()
      collection.insertOne(this.session, Document.parse(data.toJson))
      Success(1)
    } catch {
      case e: Throwable => Failure(e)
    }
  }

  private def getCollection: MongoCollection[Document] = {
    this.client.getDatabase("testdb")
      .getCollection("ReferenceVO")
  }

  private def startedTransaction(): Unit = {
    if (session == null) throw new TransactionException
  }

  override def findOne(key: String): Option[ReferenceVO] = {
    val collection = getCollection
    try {
      collection.find(Filters.eq("taskId", key)).first() match {
        case document if document == null => None
        case document: Document =>
          Some(mapper.readValue(document.toJson, classOf[ReferenceVO]))
      }
    }
  }

  override def delete(key: String): Unit = ???

  class TransactionException extends Throwable {}
}
