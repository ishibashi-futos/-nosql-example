package com.github.fishibashi.nosqlexample

import com.github.fishibashi.nosqlexample.Global.using
import org.junit.runner.RunWith
import org.scalatest.funsuite.AnyFunSuite
import org.scalatestplus.junit.JUnitRunner

import scala.util.{Failure, Success}


@RunWith(classOf[JUnitRunner])
class GlobalTest extends AnyFunSuite {
  test("開放可能なリソースを使用して処理ができる") {
    class Resource {
      def close(): Unit = println("close!")

      def add(num1: Int, num2: Int): Int = num1 + num2
    }
    using(new Resource) { r =>
      r.add(1, 1)
    } match {
      case Success(value) => assert(value == 2)
      case Failure(e) => fail(e.getMessage)
    }
  }

  test("解放時にエラーが起こる場合、Failureとして取得できる") {
    class Resource {
      def close(): Unit = {

      }

      def add(num1: Int, num2: Int): Int = throw new RuntimeException("RunTimeException")
    }
    using(new Resource) { r => {
      r.add(1, 2)
    }
    } match {
      case Success(_) => fail("ここに来たら失敗")
      case Failure(t) =>
        assert(t.getMessage == "RunTimeException")
    }
  }
}
