package com.github.fishibashi.nosqlexample.db.reference

import java.util.UUID

import com.github.fishibashi.nosqlexample.Global.using
import com.github.fishibashi.nosqlexample.util.ConnectionManager
import com.github.fishibashi.nosqlexample.vo.reference.{PointReferenceVO, ReferenceVO}
import com.mongodb.client.MongoClient
import com.mongodb.client.model.Filters
import org.bson.Document
import org.junit.runner.RunWith
import org.scalatest.BeforeAndAfter
import org.scalatest.funsuite.AnyFunSuite
import org.scalatestplus.junit.JUnitRunner
import org.slf4j.LoggerFactory

import scala.collection.immutable.Map
import scala.util.{Failure, Success}

@RunWith(classOf[JUnitRunner])
class ReferenceVOMongoRepositoryTest extends AnyFunSuite with BeforeAndAfter {

  private val logger = LoggerFactory.getLogger(classOf[ReferenceVOMongoRepositoryTest])
  private val databaseName = "testdb"
  private val collectionName = "ReferenceVO"
  private val testdata = Seq(
    Document.parse(ReferenceVO("b9cbcbd1-f038-4ca9-9df0-ed1c30da41b9", "SampleApp", "20200820000000000", "20200820235959999", 235959999L, "127.0.0.1", List(
      PointReferenceVO("b9cbcbd1-f038-4ca9-9df0-ed1c30da41b9", "cf9e924d-7a35-4160-9b42-46957f7a68b0", "15dcba89-e160-4fc7-8ce6-bf603231e76d", 0, "20200820000000000", "20200820235959999", 0, "class com.github.fishibashi.nosqlexample.db.reference.ReferenceVOPostgresRepositoryTest", "test", Map("Hello" -> "World"), "1")
    )).toJson),
    Document.parse(ReferenceVO("287f5873-722e-4bcb-8f34-a13bba9b9abc", "SampleApp", "20200820000000000", "20200820235959999", 235959999L, "127.0.0.1", List(
      PointReferenceVO("287f5873-722e-4bcb-8f34-a13bba9b9abc", "cf9e924d-7a35-4160-9b42-46957f7a68b0", "15dcba89-e160-4fc7-8ce6-bf603231e76d", 0, "20200820000000000", "20200820235959999", 0, "class com.github.fishibashi.nosqlexample.db.reference.ReferenceVOPostgresRepositoryTest", "test", Map("Hello" -> "World"), "1")
    )).toJson),
    Document.parse(ReferenceVO("5d435035-a59c-46a6-a4c7-81419a69fcd7", "SampleApp", "20200820000000000", "20200820235959999", 235959999L, "127.0.0.1", List(
      PointReferenceVO("5d435035-a59c-46a6-a4c7-81419a69fcd7", "cf9e924d-7a35-4160-9b42-46957f7a68b0", "15dcba89-e160-4fc7-8ce6-bf603231e76d", 0, "20200820000000000", "20200820235959999", 0, "class com.github.fishibashi.nosqlexample.db.reference.ReferenceVOPostgresRepositoryTest", "test", Map("Hello" -> "World"), "1")
    )).toJson),
  )
  private var client: MongoClient = _

  before {
    client = ConnectionManager.newMongoClient
    val db = client.getDatabase(databaseName)
    db.getCollection(collectionName).drop()
    logger.info("drop collection.")
    db.createCollection(collectionName)
    logger.info("create collection.")
    val collection = db.getCollection(collectionName)
    testdata.foreach(doc => collection.insertOne(doc))
  }

  after {
    if (this.client != null) client.close()
  }

  test("1件のネストしたデータ登録に成功する") {
    val refVo = ReferenceVO(
      UUID.randomUUID().toString,
      "SampleApp",
      "20200827000000000",
      "20200827235959999",
      20200827235959999L - 20200827000000000L,
      "127.0.0.1",
      List(PointReferenceVO(
        UUID.randomUUID().toString,
        UUID.randomUUID().toString,
        UUID.randomUUID().toString,
        Math.random().toLong,
        "20200827000000000",
        "20200827235959999",
        Math.random().toLong,
        this.getClass.toString,
        "test",
        Map("a" -> "String", "b" -> 100, "c" -> 100L).asInstanceOf[Map[String, AnyRef]],
        "1"
      )
      ))
    val repository = new ReferenceVOMongoRepository(client)
    val session = repository.setTransaction()
    using(session) { tran => {
      try {
        logger.info(refVo.toJson)
        repository.save(refVo)
        tran.commitTransaction()
      } catch {
        case e: Throwable =>
          tran.abortTransaction()
          throw e
      }
    }
    } match {
      case Failure(e) => throw e
      case Success(_) =>
        assert(client.getDatabase(databaseName).getCollection(collectionName).countDocuments(Filters.eq("taskId", refVo.taskId))
          == 1)
    }
  }

  test("トランザクションをabortした場合、レコードが登録されない") {
    val refVo = ReferenceVO(
      UUID.randomUUID().toString,
      "SampleApp",
      "20200827000000000",
      "20200827235959999",
      20200827235959999L - 20200827000000000L,
      "127.0.0.1",
      List(PointReferenceVO(
        UUID.randomUUID().toString,
        UUID.randomUUID().toString,
        UUID.randomUUID().toString,
        Math.random().toLong,
        "20200827000000000",
        "20200827235959999",
        Math.random().toLong,
        this.getClass.toString,
        "test",
        Map("a" -> "String", "b" -> 100, "c" -> 100L).asInstanceOf[Map[String, AnyRef]],
        "1"
      )
      ))
    val repository = new ReferenceVOMongoRepository(client)
    using(repository.setTransaction()) { tran => {
      repository.save(refVo)
      tran.abortTransaction()
    }
    } match {
      case Success(_) =>
        assert(client.getDatabase(databaseName).getCollection(collectionName).countDocuments(Filters.eq("taskId", refVo.taskId))
          == 0)
      case Failure(e) => throw e
    }
  }

  test("findに成功する") {
    val repository = new ReferenceVOMongoRepository(client)
    val taskId = "b9cbcbd1-f038-4ca9-9df0-ed1c30da41b9"
    repository.findOne(taskId) match {
      case None => fail("検索に失敗しています")
      case Some(referenceVO) =>
        assert(referenceVO.taskId == taskId)
        val table = referenceVO.referenceTable.head
        assert(table.dataTable("Hello") == "World")
    }
  }
}
