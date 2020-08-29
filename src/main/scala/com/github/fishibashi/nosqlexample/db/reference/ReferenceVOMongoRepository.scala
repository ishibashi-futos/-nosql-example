package com.github.fishibashi.nosqlexample.db.reference
import com.github.fishibashi.nosqlexample.vo.reference.ReferenceVO
import com.mongodb.ClientSessionOptions
import com.mongodb.client.{ClientSession, MongoClient, MongoCollection}
import org.bson.Document

import scala.util.{Failure, Success, Try}

class ReferenceVOMongoRepository (val client: MongoClient) extends ReferenceVORepository {

  class TransactionException extends Throwable {}

  private def getCollection: MongoCollection[Document] = {
    this.client.getDatabase("testdb")
      .getCollection("ReferenceVO")
  }

  private var session: ClientSession = _

  def setTransaction(): ClientSession = {
    this.session = client.startSession(ClientSessionOptions.builder().causallyConsistent(true).build())
    session
  }

  private def startedTransaction(): Unit = {
    if (session == null) throw new TransactionException
  }

  override def save(data: ReferenceVO): Try[Int] = {
    startedTransaction()
    val collection = getCollection
    try {
      collection.insertOne(this.session, Document.parse(data.toJson))
      Success(1)
    } catch {
      case e: Throwable => Failure(e)
    }
  }

  override def findOne(key: String): Option[ReferenceVO] = ???

  override def delete(key: String): Unit = ???
}
