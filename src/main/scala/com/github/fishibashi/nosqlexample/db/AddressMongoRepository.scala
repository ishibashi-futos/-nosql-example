package com.github.fishibashi.nosqlexample.db

import com.github.fishibashi.nosqlexample.vo.Address
import com.google.gson.Gson
import com.mongodb.client.MongoDatabase
import com.mongodb.client.model.Filters
import org.bson.Document

import scala.util.{Failure, Success, Try}

class AddressMongoRepository(val conn: MongoDatabase) extends AddressRepository {
  private val collectionName = "address"

  override def save(data: Address): Try[Int] = {
    try {
      val addresses = conn.getCollection(collectionName)
      val gson = new Gson()
      val address = Document.parse(gson.toJson(data))
      addresses.insertOne(address)
      Success(1)
    } catch {
      case e: Throwable => Failure(e)
    }
  }

  override def findOne(key: Int): Option[Address] = {
    val address = conn.getCollection(collectionName)
    address.find(Filters.eq("addressCode", key)).first() match {
      case document if document == null => None
      case document if !document.isEmpty =>
        Some(Address(addressCode = document.getInteger("addressCode"),
          prefectureCode = document.getInteger("prefectureCode"),
          municipalityCode = document.getInteger("municipalityCode"),
          areaCode = document.getInteger("areaCode"),
          zipCode = document.getString("zipCode"),
          isOffice = document.getInteger("isOffice"),
          disabled = document.getInteger("disabled"),
          prefecture = document.getString("prefecture"),
          prefectureKana = document.getString("prefectureKana"),
          municipality = document.getString("municipality"),
          municipalityKana = document.getString("municipalityKana"),
          area = document.getString("area"),
          areaKana = document.getString("areaKana"),
          areaAnnotation = document.getString("areaAnnotation"),
          kyotoStreetName = document.getString("kyotoStreetName"),
          block = document.getString("block"),
          blockKana = document.getString("blockKana"),
          remark = document.getString("remark"),
          officeName = document.getString("officeName"),
          officeNameKana = document.getString("officeNameKana"),
          officeAddress = document.getString("officeAddress"),
          newOfficeCode = document.getString("newOfficeCode"),
        ))
    }
  }

  override def delete(key: Int): Unit = {
    val address = conn.getCollection(collectionName)
    address.deleteOne(Filters.eq("addressCode", key))
  }
}
