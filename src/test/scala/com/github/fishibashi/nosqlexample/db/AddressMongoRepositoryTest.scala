package com.github.fishibashi.nosqlexample.db

import com.github.fishibashi.nosqlexample.vo.Address
import org.junit.runner.RunWith
import org.scalatest.BeforeAndAfter
import org.scalatest.funsuite.AnyFunSuite
import org.scalatestplus.junit.JUnitRunner

import scala.util.Failure

@RunWith(classOf[JUnitRunner])
class AddressMongoRepositoryTest extends AnyFunSuite with BeforeAndAfter with MongoRepositoryTest[Address] {

  private val collectionName = "address"
  private val testData = Array(
    Address(addressCode = 60850400, prefectureCode = 1, municipalityCode = 1101, areaCode = 11010000, zipCode = "060-8721", isOffice = 1, disabled = 0, prefecture = "北海道", prefectureKana = "ホッカイドウ", municipality = "札幌市中央区", municipalityKana = "サッポロシチュウオウク", area = "", areaKana = "", areaAnnotation = "（該当なし）", kyotoStreetName = "", block = "", blockKana = "", remark = "", officeName = "大丸　株式会社　紙包材営業本部", officeNameKana = "ダイマル　カブシキガイシヤ　カミホウザイエイギヨウホンブ", officeAddress = "北３条西１４丁目２番", newOfficeCode = ""),
    Address(addressCode = 60872100, prefectureCode = 1, municipalityCode = 1101, areaCode = 11010000, zipCode = "060-8602", isOffice = 1, disabled = 0, prefecture = "北海道", prefectureKana = "ホッカイドウ", municipality = "札幌市中央区", municipalityKana = "サッポロシチュウオウク", area = "", areaKana = "", areaAnnotation = "（該当なし）", kyotoStreetName = "", block = "", blockKana = "", remark = "", officeName = "株式会社　朝日新聞社　北海道支社", officeNameKana = "カブシキカイシヤ　アサヒシンブンシヤ　ホツカイドウシシヤ", officeAddress = "北１条西１丁目６番地さっぽろ創生スクエア９階", newOfficeCode = ""),
    Address(addressCode = 60860200, prefectureCode = 1, municipalityCode = 1101, areaCode = 11010000, zipCode = "060-8406", isOffice = 1, disabled = 0, prefecture = "北海道", prefectureKana = "ホッカイドウ", municipality = "札幌市中央区", municipalityKana = "サッポロシチュウオウク", area = "", areaKana = "", areaAnnotation = "（該当なし）", kyotoStreetName = "", block = "", blockKana = "", remark = "", officeName = "北海道テレビ放送　株式会社", officeNameKana = "ホツカイドウテレビホウソウ　カブシキカイシヤ", officeAddress = "北１条西１丁目６番地", newOfficeCode = ""),
    Address(addressCode = 60840600, prefectureCode = 1, municipalityCode = 1101, areaCode = 11010000, zipCode = "060-8552", isOffice = 1, disabled = 0, prefecture = "北海道", prefectureKana = "ホッカイドウ", municipality = "札幌市中央区", municipalityKana = "サッポロシチュウオウク", area = "", areaKana = "", areaAnnotation = "（該当なし）", kyotoStreetName = "", block = "", blockKana = "", remark = "", officeName = "損害保険ジャパン日本興亜　株式会社", officeNameKana = "ソンガイホケンジヤパンニツポンコウア　カブシキガイシヤ", officeAddress = "北１条西６丁目２番地損保ジャパン日本興亜札幌ビル", newOfficeCode = ""),
    Address(addressCode = 60855200, prefectureCode = 1, municipalityCode = 1101, areaCode = 11010000, zipCode = "064-8633", isOffice = 1, disabled = 0, prefecture = "北海道", prefectureKana = "ホッカイドウ", municipality = "札幌市中央区", municipalityKana = "サッポロシチュウオウク", area = "", areaKana = "", areaAnnotation = "（該当なし）", kyotoStreetName = "", block = "", blockKana = "", remark = "", officeName = "株式会社　ビッグ", officeNameKana = "カブシキガイシヤ　ビツグ", officeAddress = "南４条西７丁目６番地ビッググループ本社ビル", newOfficeCode = ""),
    Address(addressCode = 64863300, prefectureCode = 1, municipalityCode = 1101, areaCode = 11010000, zipCode = "064-8565", isOffice = 1, disabled = 0, prefecture = "北海道", prefectureKana = "ホッカイドウ", municipality = "札幌市中央区", municipalityKana = "サッポロシチュウオウク", area = "", areaKana = "", areaAnnotation = "（該当なし）", kyotoStreetName = "", block = "", blockKana = "", remark = "", officeName = "北海道トラック交通共済協同組合", officeNameKana = "ホツカイドウトラツクコウツウキヨウサイキヨウドウクミアイ", officeAddress = "南９条西１丁目１番１１号", newOfficeCode = ""),
    Address(addressCode = 64856500, prefectureCode = 1, municipalityCode = 1101, areaCode = 11010001, zipCode = "064-0941", isOffice = 0, disabled = 0, prefecture = "北海道", prefectureKana = "ホッカイドウ", municipality = "札幌市中央区", municipalityKana = "サッポロシチュウオウク", area = "旭ケ丘", areaKana = "アサヒガオカ", areaAnnotation = "", kyotoStreetName = "", block = "", blockKana = "", remark = "", officeName = "", officeNameKana = "", officeAddress = "", newOfficeCode = ""),
  )

  before {
    init()
    val addressCollection = conn.getCollection(collectionName)
    addressCollection.drop()
    setUp(collectionName, testData)
  }

  after {
    tearDown()
  }

  test("Successfully insert one.") {
    val repository = new AddressMongoRepository(conn)
    val address = Address(60000000, 2, 1101, 11010000, "060-0000", 0, 0,
      "北海道", "ホッカイドウ", "札幌市中央区", "サッポロシチュウオウク", "", "　", "（該当なし）", "", "", "", "", "", "", "", "")
    repository.save(address) match {
      case Failure(e) => fail(e.getMessage)
      case _ =>
    }
  }

  test("Successfully find one.") {
    val address: Address = testData(0)
    val repository = new AddressMongoRepository(conn)
    repository.findOne(address.addressCode) match {
      case Some(value) =>
        assert(address == value)
      case None => fail("cannot find address.")
    }
  }

  test("Specify a primary key that does not exist") {
    val repository = new AddressMongoRepository(conn)
    val address = repository.findOne(99999999)
    assert(address.isEmpty)
  }

  test("Removing an existing address") {
    val repository = new AddressMongoRepository(conn)
    val key = 64856500
    repository.delete(key)
    repository.findOne(key) match {
      case Some(_) => fail("failed to remove.")
      case None => // successfully to remove.
    }
  }

}