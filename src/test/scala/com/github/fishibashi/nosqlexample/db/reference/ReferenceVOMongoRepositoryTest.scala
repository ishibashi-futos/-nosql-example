package com.github.fishibashi.nosqlexample.db.reference

import java.util.UUID

import com.github.fishibashi.nosqlexample.Global.using
import com.github.fishibashi.nosqlexample.util.ConnectionManager
import com.github.fishibashi.nosqlexample.vo.reference.{PointReferenceVO, ReferenceVO}
import com.mongodb.client.MongoClient
import org.junit.runner.RunWith
import org.scalatest.BeforeAndAfter
import org.scalatest.funsuite.AnyFunSuite
import org.scalatestplus.junit.JUnitRunner
import org.slf4j.LoggerFactory

import scala.collection.immutable.Map
import scala.util.{Failure, Success}

@RunWith(classOf[JUnitRunner])
class ReferenceVOMongoRepositoryTest extends AnyFunSuite with BeforeAndAfter {

  private var client: MongoClient = _
  private val logger = LoggerFactory.getLogger(classOf[ReferenceVOMongoRepositoryTest])

  before {
    client = ConnectionManager.newMongoClient
    client.getDatabase("testdb").getCollection("ReferenceVO").drop()
    logger.info("drop collection.")
    client.getDatabase("testdb").createCollection("ReferenceVO")
    logger.info("create collection.")
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
      Map(1 -> PointReferenceVO(
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
    using(session) {tran => {
      try {
        tran.startTransaction()
        logger.info(refVo.toJson)
        repository.save(refVo)
        tran.commitTransaction()
      } catch {
        case e: Throwable =>
          tran.abortTransaction()
          throw e
      }
    }} match {
      case Failure(e) => throw e
      case Success(_) =>
    }
  }
}
