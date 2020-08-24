package com.github.fishibashi.nosqlexample.db

import scala.util.Try

/**
 * Repositoryの基底I/Fです.
 *
 * @tparam T 対象のmodel
 * @tparam K モデルのキー
 */
trait Repository[T, K] {
  def save(data: T): Try[Int]

  def findOne(key: K): Option[T]

  def delete(key: K): Unit
}
