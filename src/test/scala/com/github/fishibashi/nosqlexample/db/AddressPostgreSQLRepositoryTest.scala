package com.github.fishibashi.nosqlexample.db

import com.github.fishibashi.nosqlexample.vo.Address
import org.junit.runner.RunWith
import org.scalatest.BeforeAndAfter
import org.scalatest.funsuite.AnyFunSuite
import org.scalatestplus.junit.JUnitRunner

import scala.util.{Failure, Success}

@RunWith(classOf[JUnitRunner])
class AddressPostgreSQLRepositoryTest extends AnyFunSuite with BeforeAndAfter with SQLRepositoryTest {

  before {
    init()
    setup(Array("truncate table address;",
      "INSERT INTO ADDRESS(addressCode, prefectureCode, municipalityCode, areaCode, zipCode, isOffice, disabled, prefecture, prefectureKana, municipality, municipalityKana, area, areaKana, areaAnnotation, kyotoStreetName, block, blockKana, remark, officeName, officeNameKana, officeAddress, newOfficeCode) VALUES('60850400', '1', '1101', '11010000', '060-8504', '1', '0', '北海道', 'ホッカイドウ', '札幌市中央区', 'サッポロシチュウオウク', '', '　', '（該当なし）', '', '', '', '', '富士通　株式会社　北海道支社', 'フジツウ　カブシキガイシヤ　ホツカイドウシシヤ', '北２条西４丁目１番地札幌三井ＪＰビルディング', '')",
      "INSERT INTO ADDRESS(addressCode, prefectureCode, municipalityCode, areaCode, zipCode, isOffice, disabled, prefecture, prefectureKana, municipality, municipalityKana, area, areaKana, areaAnnotation, kyotoStreetName, block, blockKana, remark, officeName, officeNameKana, officeAddress, newOfficeCode) VALUES('60872100', '1', '1101', '11010000', '060-8721', '1', '0', '北海道', 'ホッカイドウ', '札幌市中央区', 'サッポロシチュウオウク', '', '　', '（該当なし）', '', '', '', '', '大丸　株式会社　紙包材営業本部', 'ダイマル　カブシキガイシヤ　カミホウザイエイギヨウホンブ', '北３条西１４丁目２番', '')",
      "INSERT INTO ADDRESS(addressCode, prefectureCode, municipalityCode, areaCode, zipCode, isOffice, disabled, prefecture, prefectureKana, municipality, municipalityKana, area, areaKana, areaAnnotation, kyotoStreetName, block, blockKana, remark, officeName, officeNameKana, officeAddress, newOfficeCode) VALUES('60860200', '1', '1101', '11010000', '060-8602', '1', '0', '北海道', 'ホッカイドウ', '札幌市中央区', 'サッポロシチュウオウク', '', '　', '（該当なし）', '', '', '', '', '株式会社　朝日新聞社　北海道支社', 'カブシキカイシヤ　アサヒシンブンシヤ　ホツカイドウシシヤ', '北１条西１丁目６番地さっぽろ創生スクエア９階', '')",
      "INSERT INTO ADDRESS(addressCode, prefectureCode, municipalityCode, areaCode, zipCode, isOffice, disabled, prefecture, prefectureKana, municipality, municipalityKana, area, areaKana, areaAnnotation, kyotoStreetName, block, blockKana, remark, officeName, officeNameKana, officeAddress, newOfficeCode) VALUES('60840600', '1', '1101', '11010000', '060-8406', '1', '0', '北海道', 'ホッカイドウ', '札幌市中央区', 'サッポロシチュウオウク', '', '　', '（該当なし）', '', '', '', '', '北海道テレビ放送　株式会社', 'ホツカイドウテレビホウソウ　カブシキカイシヤ', '北１条西１丁目６番地', '')",
      "INSERT INTO ADDRESS(addressCode, prefectureCode, municipalityCode, areaCode, zipCode, isOffice, disabled, prefecture, prefectureKana, municipality, municipalityKana, area, areaKana, areaAnnotation, kyotoStreetName, block, blockKana, remark, officeName, officeNameKana, officeAddress, newOfficeCode) VALUES('60855200', '1', '1101', '11010000', '060-8552', '1', '0', '北海道', 'ホッカイドウ', '札幌市中央区', 'サッポロシチュウオウク', '', '　', '（該当なし）', '', '', '', '', '損害保険ジャパン日本興亜　株式会社', 'ソンガイホケンジヤパンニツポンコウア　カブシキガイシヤ', '北１条西６丁目２番地損保ジャパン日本興亜札幌ビル', '')",
      "INSERT INTO ADDRESS(addressCode, prefectureCode, municipalityCode, areaCode, zipCode, isOffice, disabled, prefecture, prefectureKana, municipality, municipalityKana, area, areaKana, areaAnnotation, kyotoStreetName, block, blockKana, remark, officeName, officeNameKana, officeAddress, newOfficeCode) VALUES('64863300', '1', '1101', '11010000', '064-8633', '1', '0', '北海道', 'ホッカイドウ', '札幌市中央区', 'サッポロシチュウオウク', '', '　', '（該当なし）', '', '', '', '', '株式会社　ビッグ', 'カブシキガイシヤ　ビツグ', '南４条西７丁目６番地ビッググループ本社ビル', '')",
      "INSERT INTO ADDRESS(addressCode, prefectureCode, municipalityCode, areaCode, zipCode, isOffice, disabled, prefecture, prefectureKana, municipality, municipalityKana, area, areaKana, areaAnnotation, kyotoStreetName, block, blockKana, remark, officeName, officeNameKana, officeAddress, newOfficeCode) VALUES('64856500', '1', '1101', '11010000', '064-8565', '1', '0', '北海道', 'ホッカイドウ', '札幌市中央区', 'サッポロシチュウオウク', '', '　', '（該当なし）', '', '', '', '', '北海道トラック交通共済協同組合', 'ホツカイドウトラツクコウツウキヨウサイキヨウドウクミアイ', '南９条西１丁目１番１１号', '')",
      "INSERT INTO ADDRESS(addressCode, prefectureCode, municipalityCode, areaCode, zipCode, isOffice, disabled, prefecture, prefectureKana, municipality, municipalityKana, area, areaKana, areaAnnotation, kyotoStreetName, block, blockKana, remark, officeName, officeNameKana, officeAddress, newOfficeCode) VALUES('64094100', '1', '1101', '11010001', '064-0941', '0', '0', '北海道', 'ホッカイドウ', '札幌市中央区', 'サッポロシチュウオウク', '旭ケ丘', 'アサヒガオカ', '', '', '', '', '', '', '', '', '');",
    ))
  }

  after {
    tearDown()
  }

  test("1件のinsertに成功する") {
    val repository = new AddressPostgreSQLRepository(conn)
    val insertResult = repository.save(Address(60000000, 2, 1101, 11010000, "060-0000", 0, 0,
      "北海道", "ホッカイドウ", "札幌市中央区", "サッポロシチュウオウク", "", "　", "（該当なし）", "", "", "", "", "", "", "", ""))
    assert(insertResult.isSuccess)
    assert(insertResult.get == 1)
    conn.commit()
  }

  test("Successfully find one.") {
    val repository = new AddressPostgreSQLRepository(conn)
    val address = repository.findOne(60850400)
    assert(address.isDefined)
    address.map(address => {
      assert(address.addressCode == 60850400)
      assert(address.prefectureCode == 1)
      assert(address.municipalityCode == 1101)
      assert(address.areaCode == 11010000)
      assert(address.zipCode == "060-8504")
      assert(address.isOffice == 1)
      assert(address.disabled == 0)
      assert(address.prefecture == "北海道")
      assert(address.prefectureKana == "ホッカイドウ")
      assert(address.municipality == "札幌市中央区")
      assert(address.municipalityKana == "サッポロシチュウオウク")
      assert(address.area == "")
      assert(address.areaKana == "　")
      assert(address.areaAnnotation == "（該当なし）")
      assert(address.kyotoStreetName == "")
      assert(address.block == "")
      assert(address.blockKana == "")
      assert(address.remark == "")
      assert(address.officeName == "富士通　株式会社　北海道支社")
      assert(address.officeNameKana == "フジツウ　カブシキガイシヤ　ホツカイドウシシヤ")
      assert(address.officeAddress == "北２条西４丁目１番地札幌三井ＪＰビルディング")
      assert(address.newOfficeCode == "")
    })
  }

  test("Specify a primary key that does not exist") {
    val repository = new AddressPostgreSQLRepository(conn)
    val address = repository.findOne(99999999)
    assert(address.isEmpty)
  }

  test("update address.") {
    val repository = new AddressPostgreSQLRepository(conn)
    val address = repository.findOne(60872100).get
    assert(address.isOffice == 1)
    val address2 = address.copy(isOffice = 0)
    assert(address2.isOffice == 0)
    repository.save(address2) match {
      case Success(_) =>
      case Failure(e) => e.printStackTrace(); fail("error")
    }
    conn.commit()
    repository.findOne(60872100) match {
      case Some(result) =>
        assert(result.isOffice == 0)
        assert(result.disabled == 0)
      case None => fail("update failed.")
    }
  }

  test("Removing an existing address") {
    val repository = new AddressPostgreSQLRepository(conn)
    val key = 64094100
    repository.delete(key)
    conn.commit()
    repository.findOne(key) match {
      case Some(_) => fail("failed to remove.")
      case None => // successfully to remove.
    }
  }

  test("Passes if it is not an existing address") {
    val repository = new AddressPostgreSQLRepository(conn)
    repository.delete(99999999)
  }
}
