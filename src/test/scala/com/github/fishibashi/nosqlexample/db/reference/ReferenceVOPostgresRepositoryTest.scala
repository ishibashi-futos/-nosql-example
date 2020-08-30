package com.github.fishibashi.nosqlexample.db.reference

import java.util.UUID

import com.github.fishibashi.nosqlexample.Global.using
import com.github.fishibashi.nosqlexample.db.SQLRepositoryTest
import com.github.fishibashi.nosqlexample.vo.reference.{PointReferenceVO, ReferenceVO}
import org.junit.runner.RunWith
import org.scalatest.BeforeAndAfter
import org.scalatest.concurrent.ScalaFutures
import org.scalatest.funsuite.AnyFunSuite
import org.scalatestplus.junit.JUnitRunner

import scala.collection.immutable.Map
import scala.util.{Failure, Success}

@RunWith(classOf[JUnitRunner])
class ReferenceVOPostgresRepositoryTest extends AnyFunSuite with SQLRepositoryTest with BeforeAndAfter with ScalaFutures {
  before {
    init()
    setup(Array("TRUNCATE TABLE REFERENCEVO", "TRUNCATE TABLE POINTREFERENCEVO;"))
    setup(Array(
      "INSERT INTO referencevo (taskid, systemid, starttime, endtime, totaltime, clientip) VALUES ('b9cbcbd1-f038-4ca9-9df0-ed1c30da41b9', 'SampleApp', '20200820000000000', '20200820235959999', 235959999, '127.0.0.1');",
      "INSERT INTO pointreferencevo (taskid, referenceid, parentreferenceid, pointcount, starttime, endtime, totaltime, classname, methodname, datatable, issuccess) VALUES ('b9cbcbd1-f038-4ca9-9df0-ed1c30da41b9', 'cf9e924d-7a35-4160-9b42-46957f7a68b0', '15dcba89-e160-4fc7-8ce6-bf603231e76d', 0, '20200820000000000', '20200820235959999', 0, 'class com.github.fishibashi.nosqlexample.db.reference.ReferenceVOPostgresRepositoryTest', 'test', E'\\\\xACED0005737200237363616C612E636F6C6C656374696F6E2E696D6D757461626C652E4D6170244D61703300000000000000030200064C00297363616C6124636F6C6C656374696F6E24696D6D757461626C65244D6170244D61703324246B6579317400124C6A6176612F6C616E672F4F626A6563743B4C00297363616C6124636F6C6C656374696F6E24696D6D757461626C65244D6170244D61703324246B65793271007E00014C00297363616C6124636F6C6C656374696F6E24696D6D757461626C65244D6170244D61703324246B65793371007E00014C002B7363616C6124636F6C6C656374696F6E24696D6D757461626C65244D6170244D617033242476616C75653171007E00014C002B7363616C6124636F6C6C656374696F6E24696D6D757461626C65244D6170244D617033242476616C75653271007E00014C002B7363616C6124636F6C6C656374696F6E24696D6D757461626C65244D6170244D617033242476616C75653371007E00017870740001617400016274000163740006537472696E67737200116A6176612E6C616E672E496E746567657212E2A0A4F781873802000149000576616C7565787200106A6176612E6C616E672E4E756D62657286AC951D0B94E08B0200007870000000647372000E6A6176612E6C616E672E4C6F6E673B8BE490CC8F23DF0200014A000576616C75657871007E00080000000000000064', '1');",
      "INSERT INTO referencevo (taskid, systemid, starttime, endtime, totaltime, clientip) VALUES ('cdeb6c55-239c-479f-956c-45ab0d685037', 'SampleApp', '20200820000000000', '20200820235959999', 235959999, '127.0.0.1');",
      "INSERT INTO pointreferencevo (taskid, referenceid, parentreferenceid, pointcount, starttime, endtime, totaltime, classname, methodname, datatable, issuccess) VALUES ('cdeb6c55-239c-479f-956c-45ab0d685037', 'cf9e924d-7a35-4160-9b42-46957f7a68b0', '15dcba89-e160-4fc7-8ce6-bf603231e76d', 0, '20200820000000000', '20200820235959999', 0, 'class com.github.fishibashi.nosqlexample.db.reference.ReferenceVOPostgresRepositoryTest', 'test', E'\\\\xACED0005737200237363616C612E636F6C6C656374696F6E2E696D6D757461626C652E4D6170244D61703300000000000000030200064C00297363616C6124636F6C6C656374696F6E24696D6D757461626C65244D6170244D61703324246B6579317400124C6A6176612F6C616E672F4F626A6563743B4C00297363616C6124636F6C6C656374696F6E24696D6D757461626C65244D6170244D61703324246B65793271007E00014C00297363616C6124636F6C6C656374696F6E24696D6D757461626C65244D6170244D61703324246B65793371007E00014C002B7363616C6124636F6C6C656374696F6E24696D6D757461626C65244D6170244D617033242476616C75653171007E00014C002B7363616C6124636F6C6C656374696F6E24696D6D757461626C65244D6170244D617033242476616C75653271007E00014C002B7363616C6124636F6C6C656374696F6E24696D6D757461626C65244D6170244D617033242476616C75653371007E00017870740001617400016274000163740006537472696E67737200116A6176612E6C616E672E496E746567657212E2A0A4F781873802000149000576616C7565787200106A6176612E6C616E672E4E756D62657286AC951D0B94E08B0200007870000000647372000E6A6176612E6C616E672E4C6F6E673B8BE490CC8F23DF0200014A000576616C75657871007E00080000000000000064', '1');",
    ))
  }

  after {
    tearDown()
  }

  test("1件のネストしたデータ登録に成功する") {
    val repository = new ReferenceVOPostgresRepository(conn)
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
    repository.save(refVo)
    conn.commit()
  }

  test("テスト投入されたデータをタスクIDで検索できる") {
    val repository = new ReferenceVOPostgresRepository(conn)
    repository.findOne("b9cbcbd1-f038-4ca9-9df0-ed1c30da41b9") match {
      case Some(referenceVO) =>
        assert(referenceVO.taskId == "b9cbcbd1-f038-4ca9-9df0-ed1c30da41b9")
        val pointRefVo = referenceVO.referenceTable.head
        assert(pointRefVo.dataTable("a") == "String")
        assert(pointRefVo.dataTable("b") == 100)
        assert(pointRefVo.dataTable("c") == 100L)
      case None => fail("オブジェクトの取得に失敗しています")
    }
  }

  test("テスト投入されたデータを削除できる") {
    val repository = new ReferenceVOPostgresRepository(conn)
    val key = "cdeb6c55-239c-479f-956c-45ab0d685037"
    try {
      repository.delete(key)
    } catch {
      case t: Throwable => fail(t.getStackTrace.mkString("Array(", ", ", ")"))
    }
    conn.commit()
    repository.findOne(key) match {
      case Some(_) =>
        findPointRefVo(key) match {
          case Some(_) =>
            fail("オブジェクトを削除できていません")
          case None =>
        }
      case None =>
    }
  }

  def findPointRefVo(key: String): Option[PointReferenceVO] = {
    val sql = "SELECT * FROM pointreferencevo WHERE TASKID = ?"
    using(conn.prepareStatement(sql)) { stmt => {
      stmt.setString(1, key)
      val rs = stmt.executeQuery()
      if (rs.next()) {
        Some(PointReferenceVO(
          rs.getString("TASKID"),
          rs.getString("REFERENCEID"),
          rs.getString("PARENTREFERENCEID"),
          rs.getLong("POINTCOUNT"),
          rs.getString("STARTTIME"),
          rs.getString("ENDTIME"),
          rs.getLong("TOTALTIME"),
          rs.getString("CLASSNAME"),
          rs.getString("METHODNAME"),
          PointReferenceVO.byteArrayToDataTable(rs.getBytes("DATATABLE")),
          rs.getString("ISSUCCESS")
        ))
      } else {
        None
      }
    }
    } match {
      case Success(value) => value
      case Failure(e) => throw e
    }
  }
}
