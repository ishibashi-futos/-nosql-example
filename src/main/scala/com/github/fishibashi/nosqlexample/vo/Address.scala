package com.github.fishibashi.nosqlexample.vo

object Address {
  implicit def intFlag(arg: Int): Boolean = arg != 0
}

case class Address(addressCode: Int, // 住所コード
                   prefectureCode: Int, // 都道府県コード
                   municipalityCode: Int, // 市町村コード
                   areaCode: Int, // 町域CD
                   zipCode: String, // 郵便番号
                   isOffice: Int, // 事業所フラグ
                   disabled: Int, // 廃止フラグ
                   prefecture: String, // 都道府県
                   prefectureKana: String, // 都道府県カナ
                   municipality: String, // 市町村
                   municipalityKana: String, // 市町村カナ
                   area: String, // 町域
                   areaKana: String, // 町域カナ
                   areaAnnotation: String, // 町域注釈
                   kyotoStreetName: String, // 京都通り名
                   block: String, // 字丁目
                   blockKana: String, // 字丁目カナ
                   remark: String, // 補足
                   officeName: String, // 事業所名
                   officeNameKana: String, // 事業所名カナ
                   officeAddress: String, // 事業所住所
                   newOfficeCode: String, // 新住所CD
                  )
