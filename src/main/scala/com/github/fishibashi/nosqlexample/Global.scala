package com.github.fishibashi.nosqlexample

import org.slf4j.LoggerFactory

import scala.util.{Failure, Success, Try}

package object Global {
  private val logger = LoggerFactory.getLogger("com.github.fishibashi.nosqlexample.Global")

  def using[Resource, Result](r: Resource)(f: (Resource) => Result)(implicit RESOURCE: Resource => ({def close(): Unit})): Try[Result] = {
    try {
      val result = f(r)
      Success(result)
    } catch {
      case e: Throwable => Failure(e)
    } finally {
      try {
        r.close()
      } catch {
        case e: Throwable =>
          logger.warn("リソースの開放に失敗しました", e)
          throw e
      }
    }
  }
}
