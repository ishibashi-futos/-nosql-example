package com.github.fishibashi.nosqlexample.db

import java.sql.{Connection, PreparedStatement}

import com.github.fishibashi.nosqlexample.util.JDBCUtil
import com.github.fishibashi.nosqlexample.vo.Address

import scala.util.{Failure, Success, Try}

class AddressPostgreSQLRepository(val conn: Connection) extends Repository[Address, Int] {
  override def save(data: Address): Try[Int] = {
    val sql = "INSERT INTO ADDRESS VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)"
    var stmt: PreparedStatement = null
    try {
      stmt = conn.prepareStatement(sql)
      stmt.setInt(1, data.addressCode)
      stmt.setInt(2, data.prefectureCode)
      stmt.setInt(3, data.municipalityCode)
      stmt.setInt(4, data.areaCode)
      stmt.setString(5, data.zipCode)
      stmt.setInt(6, data.isOffice)
      stmt.setInt(7, data.disabled)
      stmt.setString(8, data.prefecture)
      stmt.setString(9, data.prefectureKana)
      stmt.setString(10, data.municipality)
      stmt.setString(11, data.municipalityKana)
      stmt.setString(12, data.area)
      stmt.setString(13, data.areaKana)
      stmt.setString(14, data.areaAnnotation)
      stmt.setString(15, data.kyotoStreetName)
      stmt.setString(16, data.block)
      stmt.setString(17, data.blockKana)
      stmt.setString(18, data.remark)
      stmt.setString(19, data.officeName)
      stmt.setString(20, data.officeNameKana)
      stmt.setString(21, data.officeAddress)
      stmt.setString(22, data.newOfficeCode)
      Success(stmt.executeUpdate())
    } catch {
      case e: Throwable => Failure(e)
    } finally {
      JDBCUtil.closeStatement(stmt)
    }
  }

  override def findOne(key: Int): Option[Address] = ???

  override def update(key: Int, data: Address): Try[Int] = ???

  override def delete(key: Int): Unit = ???
}
