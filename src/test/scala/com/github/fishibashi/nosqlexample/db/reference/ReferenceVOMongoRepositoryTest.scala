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
    Document.parse(ReferenceVO("461096cf-25a2-41e3-890c-4828d2de603c", "SampleApp", "20200801000000000", "20200801010000000", 235959999L, "127.0.0.1", List(
      PointReferenceVO("5d435035-a59c-46a6-a4c7-81419a69fcd7", "cf9e924d-7a35-4160-9b42-46957f7a68b0", "15dcba89-e160-4fc7-8ce6-bf603231e76d", 0, "20200820000000000", "20200820235959999", 0, "class com.github.fishibashi.nosqlexample.db.reference.ReferenceVOPostgresRepositoryTest", "test", Map("Hello" -> "World"), "1")
    )).toJson),
    Document.parse(ReferenceVO("96554b58-37c2-43fe-b91c-edced7959576", "SampleApp", "20200801000000000", "20200801020000000", 235959999L, "127.0.0.1", List(
      PointReferenceVO("5d435035-a59c-46a6-a4c7-81419a69fcd7", "cf9e924d-7a35-4160-9b42-46957f7a68b0", "15dcba89-e160-4fc7-8ce6-bf603231e76d", 0, "20200820000000000", "20200820235959999", 0, "class com.github.fishibashi.nosqlexample.db.reference.ReferenceVOPostgresRepositoryTest", "test", Map("Hello" -> "World"), "1")
    )).toJson),
    Document.parse(ReferenceVO("91edd7fd-03ca-43a0-822f-7379d6aeeb29", "SampleApp", "20200801000000000", "20200801030000000", 235959999L, "127.0.0.1", List(
      PointReferenceVO("5d435035-a59c-46a6-a4c7-81419a69fcd7", "cf9e924d-7a35-4160-9b42-46957f7a68b0", "15dcba89-e160-4fc7-8ce6-bf603231e76d", 0, "20200820000000000", "20200820235959999", 0, "class com.github.fishibashi.nosqlexample.db.reference.ReferenceVOPostgresRepositoryTest", "test", Map("Hello" -> "World"), "1")
    )).toJson),
    Document.parse(ReferenceVO("3cdc04c5-4198-487f-8cd6-27637ec811fe", "SampleApp", "20200801000000000", "20200801040000001", 235959999L, "127.0.0.1", List(
      PointReferenceVO("5d435035-a59c-46a6-a4c7-81419a69fcd7", "cf9e924d-7a35-4160-9b42-46957f7a68b0", "15dcba89-e160-4fc7-8ce6-bf603231e76d", 0, "20200820000000000", "20200820235959999", 0, "class com.github.fishibashi.nosqlexample.db.reference.ReferenceVOPostgresRepositoryTest", "test", Map("Hello" -> "World"), "1")
    )).toJson),
    Document.parse(ReferenceVO("6abde5a0-92b6-4c65-8381-f856100ee90b", "findBySystemIdOrderByStartTimeAsc", "20200802000000000", "20200801040000001", 235959999L, "127.0.0.1", List(
      PointReferenceVO("5d435035-a59c-46a6-a4c7-81419a69fcd7", "cf9e924d-7a35-4160-9b42-46957f7a68b0", "15dcba89-e160-4fc7-8ce6-bf603231e76d", 0, "20200820000000000", "20200820235959999", 0, "class com.github.fishibashi.nosqlexample.db.reference.ReferenceVOPostgresRepositoryTest", "test", Map("Hello" -> "World"), "1")
    )).toJson),
    Document.parse(ReferenceVO("9b5c6ea1-7b98-4275-9c25-20e7f2a0f841", "findBySystemIdOrderByStartTimeAsc", "20200803000000000", "20200801040000001", 235959999L, "127.0.0.1", List(
      PointReferenceVO("5d435035-a59c-46a6-a4c7-81419a69fcd7", "cf9e924d-7a35-4160-9b42-46957f7a68b0", "15dcba89-e160-4fc7-8ce6-bf603231e76d", 0, "20200820000000000", "20200820235959999", 0, "class com.github.fishibashi.nosqlexample.db.reference.ReferenceVOPostgresRepositoryTest", "test", Map("Hello" -> "World"), "1")
    )).toJson),
    Document.parse(ReferenceVO("de1002c7-fe8a-4281-97a1-705e9ce77da2", "findBySystemIdOrderByStartTimeAsc", "20200804000000000", "20200801040000001", 235959999L, "127.0.0.1", List(
      PointReferenceVO("5d435035-a59c-46a6-a4c7-81419a69fcd7", "cf9e924d-7a35-4160-9b42-46957f7a68b0", "15dcba89-e160-4fc7-8ce6-bf603231e76d", 0, "20200820000000000", "20200820235959999", 0, "class com.github.fishibashi.nosqlexample.db.reference.ReferenceVOPostgresRepositoryTest", "test", Map("Hello" -> "World"), "1")
    )).toJson),
    Document.parse(ReferenceVO("a9e4d9ff-ec7f-47b9-9e46-dce33c9c242b", "SampleWebApp1", "20200804000000000", "20200801040000001", 235959999L, "127.0.0.1", List(
      PointReferenceVO("5d435035-a59c-46a6-a4c7-81419a69fcd7", "cf9e924d-7a35-4160-9b42-46957f7a68b0", "15dcba89-e160-4fc7-8ce6-bf603231e76d", 0, "20200820000000000", "20200820235959999", 0, "class com.github.fishibashi.nosqlexample.db.reference.ReferenceVOPostgresRepositoryTest", "test", Map("Hello" -> "World"), "1")
    )).toJson),
    Document.parse(ReferenceVO("474f76b4-3e4e-49e8-96db-65be87052ca2", "SampleWebApp2", "20200804000000000", "20200801040000001", 235959999L, "127.0.0.1", List(
      PointReferenceVO("5d435035-a59c-46a6-a4c7-81419a69fcd7", "cf9e924d-7a35-4160-9b42-46957f7a68b0", "15dcba89-e160-4fc7-8ce6-bf603231e76d", 0, "20200820000000000", "20200820235959999", 0, "class com.github.fishibashi.nosqlexample.db.reference.ReferenceVOPostgresRepositoryTest", "test", Map("Hello" -> "World"), "1")
    )).toJson),
    Document.parse(ReferenceVO("7109ae6c-c221-4827-9b18-b771d8ff4a07", "SampleWebApp2", "20200804000000000", "20200801040000001", 235959999L, "127.0.0.1", List(
      PointReferenceVO("5d435035-a59c-46a6-a4c7-81419a69fcd7", "cf9e924d-7a35-4160-9b42-46957f7a68b0", "15dcba89-e160-4fc7-8ce6-bf603231e76d", 0, "20200820000000000", "20200820235959999", 0, "class com.github.fishibashi.nosqlexample.db.reference.ReferenceVOPostgresRepositoryTest", "test", Map("Hello" -> "World"), "1")
    )).toJson),
    Document.parse(ReferenceVO("ba56dd52-0e5c-4ecb-bef8-bc8d11977a67", "SampleWebApp3", "20200804000000000", "20200801040000001", 235959999L, "127.0.0.1", List(
      PointReferenceVO("5d435035-a59c-46a6-a4c7-81419a69fcd7", "cf9e924d-7a35-4160-9b42-46957f7a68b0", "15dcba89-e160-4fc7-8ce6-bf603231e76d", 0, "20200820000000000", "20200820235959999", 0, "class com.github.fishibashi.nosqlexample.db.reference.ReferenceVOPostgresRepositoryTest", "test", Map("Hello" -> "World"), "1")
    )).toJson),
    Document.parse(ReferenceVO("42b0a7b6-b9d4-4f81-8027-115f3e9f059c", "SampleWebApp3", "20200804000000000", "20200801040000001", 235959999L, "127.0.0.1", List(
      PointReferenceVO("5d435035-a59c-46a6-a4c7-81419a69fcd7", "cf9e924d-7a35-4160-9b42-46957f7a68b0", "15dcba89-e160-4fc7-8ce6-bf603231e76d", 0, "20200820000000000", "20200820235959999", 0, "class com.github.fishibashi.nosqlexample.db.reference.ReferenceVOPostgresRepositoryTest", "test", Map("Hello" -> "World"), "1")
    )).toJson),
    Document.parse(ReferenceVO("e7520b35-260b-4f89-9de3-7f07bcc5dc8a", "SampleWebApp3", "20200804000000000", "20200801040000001", 235959999L, "127.0.0.1", List(
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

  test("1件の削除に成功する") {
    val repository = new ReferenceVOMongoRepository(client)
    val taskId = "5d435035-a59c-46a6-a4c7-81419a69fcd7"
    using(repository.setTransaction()) { stmt =>
      repository.delete(taskId)
      stmt.commitTransaction()
    }

    repository.findOne(taskId) match {
      case Some(_) => fail("削除できていない")
      case None => //OK
    }
  }

  test("削除のトランザクションをロールバックできる") {
    val repository = new ReferenceVOMongoRepository(client)
    val taskId = "5d435035-a59c-46a6-a4c7-81419a69fcd7"
    using(repository.setTransaction()) { stmt =>
      repository.delete(taskId)
      stmt.abortTransaction()
    }

    repository.findOne(taskId) match {
      case Some(_) => // OK
      case None => fail("Rollbackできてない")
    }
  }

  test("開始時間と終了時間で絞り込める") {
    val repository = new ReferenceVOMongoRepository(client)
    repository.findByStartTimeAndEndTime("20200801000000000", "20200801030000000") match {
      case None => fail("検索できていません")
      case Some(list) =>
        assert(list.size == 3)
    }
  }

  test("開始時間と終了時間で絞り込んで結果が０件の場合、エラーとならずNoneが返却される") {
    val repository = new ReferenceVOMongoRepository(client)
    repository.findByStartTimeAndEndTime("19921116000000000", "19921116235959999") match {
      case None => // OK
      case Some(list) =>
        fail("絞り込めていません")
    }
  }

  test("systemIdで絞り込んだ後、startTimeで昇順にソートできる") {
    val repository = new ReferenceVOMongoRepository(client)
    repository.findBySystemIdOrderByStartTimeAsc("findBySystemIdOrderByStartTimeAsc") match {
      case None => fail("検索に失敗")
      case Some(list) =>
        assert(list.head.startTime == "20200802000000000")
        assert(list(1).startTime == "20200803000000000")
        assert(list(2).startTime == "20200804000000000")
    }
  }

  test("systemIdで絞り込んだ後、startTimeで降順にソートできる") {
    val repository = new ReferenceVOMongoRepository(client)
    repository.findBySystemIdOrderByStartTimeDesc("findBySystemIdOrderByStartTimeAsc") match {
      case None => fail("検索に失敗")
      case Some(list) =>
        assert(list.head.startTime == "20200804000000000")
        assert(list(1).startTime == "20200803000000000")
        assert(list(2).startTime == "20200802000000000")
    }
  }

  test("in句を使用した検索ができる") {
    val repository = new ReferenceVOMongoRepository(client)
    val result = repository.findBySystemIds(List("SampleWebApp1", "SampleWebApp2", "SampleWebApp3"))
    val check = (systemId: String, size: Int) => {
      result.get(systemId) match {
        case None => fail("Error")
        case Some(list) =>
          assert(list.size == size)
      }
    }
    check("SampleWebApp1", 1)
    check("SampleWebApp2", 2)
    check("SampleWebApp3", 3)
  }

  test("taskIdのリストに合致するレコードが検索できる") {
    val taskIds = List("b9cbcbd1-f038-4ca9-9df0-ed1c30da41b9",
      "287f5873-722e-4bcb-8f34-a13bba9b9abc",
      "5d435035-a59c-46a6-a4c7-81419a69fcd7",
    )

    val repository = new ReferenceVOMongoRepository(client)
    using(repository.setTransaction()) {session =>
      session.startTransaction()
      assert(repository.findByTaskIds(taskIds).size == taskIds.size)
      session.abortTransaction()
    }
  }

  test("taskIdのリストに一致しないtaskIdがあった場合、一致するレコードのみ返却される") {
    val taskIds = List("b9cbcbd1-f038-4ca9-9df0-ed1c30da4110", // 不一致
      "287f5873-722e-4bcb-8f34-a13bba9b9abc",
      "5d435035-a59c-46a6-a4c7-81419a69fcd7",
    )

    val repository = new ReferenceVOMongoRepository(client)
    using(repository.setTransaction()) {session =>
      session.startTransaction()
      assert(repository.findByTaskIds(taskIds).size == 2)
      session.abortTransaction()
    }
  }

  test("複数のレコードを登録し、存在しないtaskIdのレコードのみ登録されること") {
    val refVoList = List(
      // 既に存在するため無視されるであろうレコード
      ReferenceVO("b9cbcbd1-f038-4ca9-9df0-ed1c30da41b9", "SampleApp", "20200820000000000", "20200820235959999", 235959999L, "127.0.0.1", List(
        PointReferenceVO("b9cbcbd1-f038-4ca9-9df0-ed1c30da41b9", "cf9e924d-7a35-4160-9b42-46957f7a68b0", "15dcba89-e160-4fc7-8ce6-bf603231e76d", 0, "20200820000000000", "20200820235959999", 0, "class com.github.fishibashi.nosqlexample.db.reference.ReferenceVOPostgresRepositoryTest", "test", Map("Hello" -> "World"), "1")
      )),
      ReferenceVO("6c198760-bb53-4bee-90e0-b5d38cf86bf2", "SampleApp", "20200820000000000", "20200820235959999", 235959999L, "127.0.0.1", List(
        PointReferenceVO("b9cbcbd1-f038-4ca9-9df0-ed1c30da41b9", "cf9e924d-7a35-4160-9b42-46957f7a68b0", "15dcba89-e160-4fc7-8ce6-bf603231e76d", 0, "20200820000000000", "20200820235959999", 0, "class com.github.fishibashi.nosqlexample.db.reference.ReferenceVOPostgresRepositoryTest", "test", Map("Hello" -> "World"), "1")
      )),
      ReferenceVO("b4d114c1-f2a4-4d01-a59f-35ff148906d5", "SampleApp", "20200820000000000", "20200820235959999", 235959999L, "127.0.0.1", List(
        PointReferenceVO("b9cbcbd1-f038-4ca9-9df0-ed1c30da41b9", "cf9e924d-7a35-4160-9b42-46957f7a68b0", "15dcba89-e160-4fc7-8ce6-bf603231e76d", 0, "20200820000000000", "20200820235959999", 0, "class com.github.fishibashi.nosqlexample.db.reference.ReferenceVOPostgresRepositoryTest", "test", Map("Hello" -> "World"), "1")
      )),
    )
    val taskIds = refVoList.map(refVo => refVo.taskId)
    val repository = new ReferenceVOMongoRepository(client)
    using(repository.setTransaction()) {session =>
      session.startTransaction()
      assert(repository.insertMany(taskIds, refVoList) == 2)
      assert(repository.findByTaskIds(taskIds).size == taskIds.size)
      session.commitTransaction()
    }
  }

  test("複数のレコードを登録し、Rollbackした場合、新規登録のレコードのみRollbackされること") {
    val refVoList = List(
      // 既に存在するため無視されるであろうレコード
      ReferenceVO("b9cbcbd1-f038-4ca9-9df0-ed1c30da41b9", "SampleApp", "20200820000000000", "20200820235959999", 235959999L, "127.0.0.1", List(
        PointReferenceVO("b9cbcbd1-f038-4ca9-9df0-ed1c30da41b9", "cf9e924d-7a35-4160-9b42-46957f7a68b0", "15dcba89-e160-4fc7-8ce6-bf603231e76d", 0, "20200820000000000", "20200820235959999", 0, "class com.github.fishibashi.nosqlexample.db.reference.ReferenceVOPostgresRepositoryTest", "test", Map("Hello" -> "World"), "1")
      )),
      // 以下、ROLLBACKされるレコード
      ReferenceVO("6c198760-bb53-4bee-90e0-b5d38cf86bf2", "SampleApp", "20200820000000000", "20200820235959999", 235959999L, "127.0.0.1", List(
        PointReferenceVO("b9cbcbd1-f038-4ca9-9df0-ed1c30da41b9", "cf9e924d-7a35-4160-9b42-46957f7a68b0", "15dcba89-e160-4fc7-8ce6-bf603231e76d", 0, "20200820000000000", "20200820235959999", 0, "class com.github.fishibashi.nosqlexample.db.reference.ReferenceVOPostgresRepositoryTest", "test", Map("Hello" -> "World"), "1")
      )),
      ReferenceVO("b4d114c1-f2a4-4d01-a59f-35ff148906d5", "SampleApp", "20200820000000000", "20200820235959999", 235959999L, "127.0.0.1", List(
        PointReferenceVO("b9cbcbd1-f038-4ca9-9df0-ed1c30da41b9", "cf9e924d-7a35-4160-9b42-46957f7a68b0", "15dcba89-e160-4fc7-8ce6-bf603231e76d", 0, "20200820000000000", "20200820235959999", 0, "class com.github.fishibashi.nosqlexample.db.reference.ReferenceVOPostgresRepositoryTest", "test", Map("Hello" -> "World"), "1")
      )),
    )
    val taskIds = refVoList.map(refVo => refVo.taskId)

    val repository = new ReferenceVOMongoRepository(client)
    using(repository.setTransaction()) {session =>
      session.startTransaction()
      assert(repository.insertMany(taskIds, refVoList) == 2)
      // rollback
      session.abortTransaction()
    }

    val collection = client.getDatabase("testdb").getCollection("ReferenceVO")
    val count = (taskId: String) =>
      collection.countDocuments(Filters.eq("taskId", taskId))

    assert(count("b9cbcbd1-f038-4ca9-9df0-ed1c30da41b9") == 1)
    assert(count("6c198760-bb53-4bee-90e0-b5d38cf86bf2") == 0)
    assert(count("b4d114c1-f2a4-4d01-a59f-35ff148906d5") == 0)
  }}
