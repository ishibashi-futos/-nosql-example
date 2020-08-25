package com.github.fishibashi.nosqlexample.db

import com.couchbase.client.java.json.JsonObject
import com.couchbase.client.java.{Bucket, Collection}
import com.fasterxml.jackson.databind.ObjectMapper
import com.github.fishibashi.nosqlexample.vo.Address

import scala.util.{Failure, Success, Try}

class AddressCouchRepository extends AddressRepository {

  private val collectionName = "address"
  private val mapper = new ObjectMapper()
  private var col: Collection = _

  def this(bucket: Bucket) {
    this()
    this.col = bucket.defaultCollection()
  }

  override def save(data: Address): Try[Int] = {
    try {
      val address = JsonObject.fromJson(mapper.writeValueAsString(data))
      col.insert(data.addressCode.toString, address)
      Success(1)
    } catch {
      case e: Throwable => Failure(e)
    }
  }

  override def findOne(key: Int): Option[Address] = ???

  override def delete(key: Int): Unit = ???
}
