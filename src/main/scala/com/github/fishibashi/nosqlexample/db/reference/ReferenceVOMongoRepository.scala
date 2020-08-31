package com.github.fishibashi.nosqlexample.db.reference

import com.github.fishibashi.nosqlexample.util.DefaultMapperConfig
import com.github.fishibashi.nosqlexample.vo.reference.ReferenceVO
import com.mongodb.client.model.Filters
import com.mongodb.client.{ClientSession, MongoClient, MongoCollection}
import com.mongodb.{BasicDBObject, ClientSessionOptions}
import org.bson.Document

import scala.jdk.CollectionConverters.{IterableHasAsJava, IterableHasAsScala}
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
    getCollection.find(Filters.eq("taskId", key)).first() match {
      case document if document == null => None
      case document: Document =>
        Some(mapper.readValue(document.toJson, classOf[ReferenceVO]))
    }
  }

  override def delete(key: String): Unit = {
    val collection = getCollection
    this.findOne(key) match {
      case Some(_) =>
        session.startTransaction()
        collection.deleteOne(session, Filters.eq("taskId", key))
      case None =>
    }
  }

  def findByStartTimeAndEndTime(startTime: String, endTime: String): Option[List[ReferenceVO]] = {
    getCollection.find(Filters.and(Filters.gte("startTime", startTime), Filters.lte("endTime", endTime)))
      .map(doc => {
        mapper.readValue(doc.toJson, classOf[ReferenceVO])
      }).asScala.toList match {
      case list if list.isEmpty => None
      case list => Some(list)
    }
  }

  def findBySystemIdOrderByStartTimeAsc(systemId: String): Option[List[ReferenceVO]] = {
    getCollection.find(Filters.eq("systemId", systemId)).sort(new BasicDBObject("startTime", 1))
      .map(doc => mapper.readValue(doc.toJson, classOf[ReferenceVO])).asScala.toList match {
      case list if list.isEmpty => None
      case list => Some(list)
    }
  }

  def findBySystemIdOrderByStartTimeDesc(systemId: String): Option[List[ReferenceVO]] = {
    getCollection.find(Filters.eq("systemId", systemId)).sort(new BasicDBObject("startTime", -1))
      .map(doc => mapper.readValue(doc.toJson, classOf[ReferenceVO])).asScala.toList match {
      case list if list.isEmpty => None
      case list => Some(list)
    }
  }

  def findBySystemIds(systemIds: List[String]): Map[String, List[ReferenceVO]] = {
    val collections = getCollection.find(Filters.in("systemId", systemIds.asJava))
      .sort(new BasicDBObject("systemId", 1))
      .map(doc => mapper.readValue(doc.toJson, classOf[ReferenceVO])).asScala.toList
    systemIds.map(systemId => {
      (systemId, collections.filter(refVo => refVo.systemId == systemId))
    }).toMap
  }

  def insertMany(taskIds: List[String], referenceVoList: List[ReferenceVO]): Int = {
    val savedList = findByTaskIds(taskIds)
    val notSavedList = referenceVoList.filter(refVo => !savedList.contains(refVo.taskId))
    notSavedList.foreach(_ => save(_))
    notSavedList.size
  }

  def findByTaskIds(taskIds: List[String]): List[ReferenceVO] = {
    getCollection.find(session, Filters.in("taskId", taskIds.asJava)).map(doc => mapper.readValue(doc.toJson, classOf[ReferenceVO]))
      .asScala.toList
  }

  class TransactionException extends Throwable {}

}
