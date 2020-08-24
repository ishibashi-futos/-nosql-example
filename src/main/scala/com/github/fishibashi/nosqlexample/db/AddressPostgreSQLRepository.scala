package com.github.fishibashi.nosqlexample.db

import java.sql.{Connection, PreparedStatement, ResultSet}

import com.github.fishibashi.nosqlexample.util.DriverUtil
import com.github.fishibashi.nosqlexample.vo.Address

import scala.util.{Failure, Success, Try}

class AddressPostgreSQLRepository(val conn: Connection) extends AddressRepository {
  override def save(data: Address): Try[Int] = {
    findOne(data.addressCode) match {
      case Some(_) => update(data)
      case None => insert(data)
    }
  }

  private def insert(address: Address): Try[Int] = {
    val sql = "INSERT INTO ADDRESS VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)"
    var stmt: PreparedStatement = null
    try {
      stmt = conn.prepareStatement(sql)
      stmt.setInt(1, address.addressCode)
      stmt.setInt(2, address.prefectureCode)
      stmt.setInt(3, address.municipalityCode)
      stmt.setInt(4, address.areaCode)
      stmt.setString(5, address.zipCode)
      stmt.setInt(6, address.isOffice)
      stmt.setInt(7, address.disabled)
      stmt.setString(8, address.prefecture)
      stmt.setString(9, address.prefectureKana)
      stmt.setString(10, address.municipality)
      stmt.setString(11, address.municipalityKana)
      stmt.setString(12, address.area)
      stmt.setString(13, address.areaKana)
      stmt.setString(14, address.areaAnnotation)
      stmt.setString(15, address.kyotoStreetName)
      stmt.setString(16, address.block)
      stmt.setString(17, address.blockKana)
      stmt.setString(18, address.remark)
      stmt.setString(19, address.officeName)
      stmt.setString(20, address.officeNameKana)
      stmt.setString(21, address.officeAddress)
      stmt.setString(22, address.newOfficeCode)
      Success(stmt.executeUpdate())
    } catch {
      case e: Throwable => Failure(e)
    } finally {
      DriverUtil.closeStatement(stmt)
    }
  }

  private def update(address: Address): Try[Int] = {
    val sql = "UPDATE ADDRESS SET " +
      "prefectureCode = ? " +
      ",municipalityCode = ? " +
      ",areaCode = ? " +
      ",zipCode = ? " +
      ",isOffice = ? " +
      ",disabled = ? " +
      ",prefecture = ? " +
      ",prefectureKana = ? " +
      ",municipality = ? " +
      ",municipalityKana = ? " +
      ",area = ? " +
      ",areaKana = ? " +
      ",areaAnnotation = ? " +
      ",kyotoStreetName = ? " +
      ",block = ? " +
      ",blockKana = ? " +
      ",remark = ? " +
      ",officeName = ? " +
      ",officeNameKana = ? " +
      ",officeAddress = ? " +
      ",newOfficeCode = ? " +
      "WHERE addressCode = ?"
    var stmt: PreparedStatement = null
    try {
      stmt = conn.prepareStatement(sql)
      stmt.setInt(1, address.prefectureCode)
      stmt.setInt(2, address.municipalityCode)
      stmt.setInt(3, address.areaCode)
      stmt.setString(4, address.zipCode)
      stmt.setInt(5, address.isOffice)
      stmt.setInt(6, address.disabled)
      stmt.setString(7, address.prefecture)
      stmt.setString(8, address.prefectureKana)
      stmt.setString(9, address.municipality)
      stmt.setString(10, address.municipalityKana)
      stmt.setString(11, address.area)
      stmt.setString(12, address.areaKana)
      stmt.setString(13, address.areaAnnotation)
      stmt.setString(14, address.kyotoStreetName)
      stmt.setString(15, address.block)
      stmt.setString(16, address.blockKana)
      stmt.setString(17, address.remark)
      stmt.setString(18, address.officeName)
      stmt.setString(19, address.officeNameKana)
      stmt.setString(20, address.officeAddress)
      stmt.setString(21, address.newOfficeCode)
      stmt.setInt(22, address.addressCode)
      Success(stmt.executeUpdate())
    } catch {
      case e: Throwable => Failure(e)
    } finally {
      DriverUtil.closeStatement(stmt)
    }
  }

  override def findOne(key: Int): Option[Address] = {
    val sql = "SELECT * FROM ADDRESS WHERE addressCode = ?"
    var stmt: PreparedStatement = null
    var rs: ResultSet = null
    try {
      stmt = conn.prepareStatement(sql)
      stmt.setInt(1, key)
      rs = stmt.executeQuery()
      while (rs.next()) {
        return Some(Address(rs.getInt("addressCode"),
          rs.getInt("prefectureCode"),
          rs.getInt("municipalityCode"),
          rs.getInt("areaCode"),
          rs.getString("zipCode"),
          rs.getInt("isOffice"),
          rs.getInt("disabled"),
          rs.getString("prefecture"),
          rs.getString("prefectureKana"),
          rs.getString("municipality"),
          rs.getString("municipalityKana"),
          rs.getString("area"),
          rs.getString("areaKana"),
          rs.getString("areaAnnotation"),
          rs.getString("kyotoStreetName"),
          rs.getString("block"),
          rs.getString("blockKana"),
          rs.getString("remark"),
          rs.getString("officeName"),
          rs.getString("officeNameKana"),
          rs.getString("officeAddress"),
          rs.getString("newOfficeCode")))
      }
      None
    } catch {
      case e: Throwable => throw e
    } finally {
      DriverUtil.closeStatement(stmt)
      if (rs != null) {
        rs.close()
      }
    }
  }

  override def delete(key: Int): Unit = {
    findOne(key) match {
      case Some(_) =>
        val sql = "DELETE FROM ADDRESS WHERE addressCode = ?"
        var stmt: PreparedStatement = null
        try {
          stmt = conn.prepareStatement(sql)
          stmt.setInt(1, key)
          stmt.executeUpdate()
        } finally {
          DriverUtil.closeStatement(stmt)
        }
      case None =>
    }
  }
}
