package com.github.fishibashi.nosqlexample.util

import org.junit.runner.RunWith
import org.scalatest.funsuite.AnyFunSuite
import org.scalatestplus.junit.JUnitRunner

@RunWith(classOf[JUnitRunner])
class ConnectionManagerTest extends AnyFunSuite {
  test("getNewConnection") {
    val conn = ConnectionManager.newSqlConnection()
    assert(Some(conn).isDefined)
  }
}
