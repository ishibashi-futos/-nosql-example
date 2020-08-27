package com.github.fishibashi.nosqlexample

import scala.util.{Failure, Success, Try}

object Global {
  def using[Resource, Result](r: Resource)(f: (Resource) => Result)(implicit RESOURCE: Resource => ({def close(): Unit})): Try[Result] = {
    try Success(f(r)) catch {
      case e: Throwable => Failure(e)
    } finally {
      r.close()
    }
  }
}
