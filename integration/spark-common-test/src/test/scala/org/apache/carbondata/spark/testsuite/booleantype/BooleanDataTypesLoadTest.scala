/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.carbondata.spark.testsuite.booleantype

import java.io.File

import org.apache.spark.sql.Row
import org.apache.spark.sql.test.util.QueryTest
import org.scalatest.{BeforeAndAfterAll, BeforeAndAfterEach}

/**
  * Created by root on 9/17/17.
  */
class BooleanDataTypesLoadTest extends QueryTest with BeforeAndAfterEach with BeforeAndAfterAll {
  val rootPath = new File(this.getClass.getResource("/").getPath
    + "../../../..").getCanonicalPath

  override def beforeEach(): Unit = {
    sql("drop table if exists carbon_table")
    sql("drop table if exists boolean_table")
    sql("drop table if exists boolean_table2")
    sql("drop table if exists boolean_table3")
    sql("drop table if exists boolean_table4")
    sql("CREATE TABLE if not exists carbon_table(booleanField BOOLEAN) STORED BY 'carbondata'")
  }

  override def afterAll(): Unit = {
    sql("drop table if exists carbon_table")
    sql("drop table if exists boolean_table")
    sql("drop table if exists boolean_table2")
    sql("drop table if exists boolean_table3")
    sql("drop table if exists boolean_table4")
  }

  test("Loading table: support boolean data type format") {
    val fileLocation = s"$rootPath/integration/spark-common-test/src/test/resources/bool/supportBooleanOnlyBoolean.csv"
    sql(
      s"""
         | LOAD DATA LOCAL INPATH '$fileLocation'
         | INTO TABLE carbon_table
         | OPTIONS('FILEHEADER' = 'booleanField')
       """.stripMargin)

    checkAnswer(sql("select * from carbon_table where booleanField = true"),
      Seq(Row(true), Row(true), Row(true), Row(true)))

    checkAnswer(sql("select * from carbon_table"),
      Seq(Row(true), Row(true), Row(true), Row(true), Row(false), Row(false), Row(false), Row(false), Row(null), Row(null), Row(null)))
  }

  test("Loading table: support boolean data type format, different format") {
    val fileLocation = s"$rootPath/integration/spark-common-test/src/test/resources/bool/supportBooleanDifferentFormat.csv"
    sql(
      s"""
         | LOAD DATA LOCAL INPATH '$fileLocation'
         | INTO TABLE carbon_table
         | OPTIONS('FILEHEADER' = 'booleanField')
       """.stripMargin)

    checkAnswer(sql("select * from carbon_table where booleanField = true"),
      Seq(Row(true), Row(true), Row(true), Row(true)))

    checkAnswer(sql("select * from carbon_table"),
      Seq(Row(true), Row(true), Row(true), Row(true), Row(false), Row(false), Row(false), Row(false)
        , Row(null), Row(null), Row(null), Row(null), Row(null), Row(null), Row(null), Row(null), Row(null), Row(null), Row(null), Row(null)))
  }

  test("Loading table: support boolean and other data type") {
    sql(
      s"""
         | CREATE TABLE boolean_table(
         | shortField SHORT,
         | booleanField BOOLEAN,
         | intField INT,
         | bigintField LONG,
         | doubleField DOUBLE,
         | stringField STRING,
         | timestampField TIMESTAMP,
         | decimalField DECIMAL(18,2),
         | dateField DATE,
         | charField CHAR(5),
         | floatField FLOAT,
         | complexData ARRAY<STRING>,
         | booleanField2 BOOLEAN
         | )
         | STORED BY 'carbondata'
         | TBLPROPERTIES('sort_columns'='','DICTIONARY_INCLUDE'='dateField, charField')
       """.stripMargin)

    val storeLocation = s"$rootPath/integration/spark-common-test/src/test/resources/bool/supportBoolean.csv"
    sql(
      s"""
         | LOAD DATA LOCAL INPATH '${storeLocation}'
         | INTO TABLE boolean_table
         | options('FILEHEADER'='shortField,booleanField,intField,bigintField,doubleField,stringField,timestampField,decimalField,dateField,charField,floatField,complexData,booleanField2')
           """.stripMargin)

    checkAnswer(
      sql("select booleanField,intField from boolean_table"),
      Seq(Row(true, 10), Row(false, 17), Row(false, 11),
        Row(true, 10), Row(true, 10), Row(true, 14),
        Row(false, 10), Row(false, 10), Row(false, 16), Row(false, 10))
    )
  }

  test("Loading table: data columns is less than table defined columns") {

    sql(
      s"""
         | CREATE TABLE boolean_table(
         | shortField SHORT,
         | booleanField BOOLEAN,
         | intField INT,
         | bigintField LONG,
         | doubleField DOUBLE,
         | stringField STRING,
         | timestampField TIMESTAMP,
         | decimalField DECIMAL(18,2),
         | dateField DATE,
         | charField CHAR(5),
         | floatField FLOAT,
         | complexData ARRAY<STRING>,
         | booleanField2 BOOLEAN
         | )
         | STORED BY 'carbondata'
         | TBLPROPERTIES('sort_columns'='','DICTIONARY_INCLUDE'='dateField, charField')
       """.stripMargin)

    val storeLocation = s"$rootPath/integration/spark-common-test/src/test/resources/bool/supportBoolean.csv"
    sql(
      s"""
         | LOAD DATA LOCAL INPATH '${storeLocation}'
         | INTO TABLE boolean_table
         | options('FILEHEADER'='shortField,booleanField,intField,bigintField,doubleField,stringField,timestampField,decimalField,dateField,charField,floatField,complexData,booleanField2')
           """.stripMargin)
    checkAnswer(
      sql("select booleanField,intField,booleanField2 from boolean_table"),
      Seq(Row(true, 10, null), Row(false, 17, null), Row(false, 11, null),
        Row(true, 10, null), Row(true, 10, null), Row(true, 14, null),
        Row(false, 10, null), Row(false, 10, null), Row(false, 16, null), Row(false, 10, null))
    )
  }

  test("Loading table: support boolean and other data type, data columns bigger than table defined columns") {
    sql(
      s"""
         | CREATE TABLE boolean_table(
         | shortField SHORT,
         | booleanField BOOLEAN,
         | intField INT,
         | bigintField LONG,
         | doubleField DOUBLE,
         | stringField STRING,
         | timestampField TIMESTAMP,
         | decimalField DECIMAL(18,2),
         | dateField DATE,
         | charField CHAR(5),
         | floatField FLOAT,
         | complexData ARRAY<STRING>
         | )
         | STORED BY 'carbondata'
         | TBLPROPERTIES('sort_columns'='','DICTIONARY_INCLUDE'='dateField, charField')
       """.stripMargin)

    val storeLocation = s"$rootPath/integration/spark-common-test/src/test/resources/bool/supportBooleanTwoBooleanColumns.csv"
    sql(
      s"""
         | LOAD DATA LOCAL INPATH '${storeLocation}'
         | INTO TABLE boolean_table
         | options('FILEHEADER'='shortField,booleanField,intField,bigintField,doubleField,stringField,timestampField,decimalField,dateField,charField,floatField,complexData,booleanField2')
           """.stripMargin)

    checkAnswer(
      sql("select booleanField,intField from boolean_table"),
      Seq(Row(true, 10), Row(false, 17), Row(false, 11),
        Row(true, 10), Row(true, 10), Row(true, 14),
        Row(false, 10), Row(false, 10), Row(false, 16), Row(false, 10))
    )
  }

  test("Loading table: support boolean and other data type, with file header") {
    sql(
      s"""
         | CREATE TABLE boolean_table(
         | shortField SHORT,
         | booleanField BOOLEAN,
         | intField INT,
         | bigintField LONG,
         | doubleField DOUBLE,
         | stringField STRING,
         | timestampField TIMESTAMP,
         | decimalField DECIMAL(18,2),
         | dateField DATE,
         | charField CHAR(5),
         | floatField FLOAT,
         | complexData ARRAY<STRING>
         | )
         | STORED BY 'carbondata'
         | TBLPROPERTIES('sort_columns'='','DICTIONARY_INCLUDE'='dateField, charField')
       """.stripMargin)

    val storeLocation = s"$rootPath/integration/spark-common-test/src/test/resources/bool/supportBooleanWithFileHeader.csv"
    sql(
      s"""
         | LOAD DATA LOCAL INPATH '${storeLocation}'
         | INTO TABLE boolean_table
           """.stripMargin)

    checkAnswer(
      sql("select booleanField,intField from boolean_table"),
      Seq(Row(true, 10), Row(false, 17), Row(false, 11),
        Row(true, 10), Row(true, 10), Row(true, 14),
        Row(false, 10), Row(false, 10), Row(false, 16), Row(false, 10))
    )
  }

  test("Loading table: create with DICTIONARY_EXCLUDE, TABLE_BLOCKSIZE, NO_INVERTED_INDEX, SORT_SCOPE") {
    sql(
      s"""
         | CREATE TABLE boolean_table(
         | shortField SHORT,
         | booleanField BOOLEAN,
         | intField INT,
         | bigintField LONG,
         | doubleField DOUBLE,
         | stringField STRING,
         | timestampField TIMESTAMP,
         | decimalField DECIMAL(18,2),
         | dateField DATE,
         | charField CHAR(5),
         | floatField FLOAT,
         | complexData ARRAY<STRING>,
         | booleanField2 BOOLEAN
         | )
         | STORED BY 'carbondata'
         | TBLPROPERTIES('sort_columns'='','DICTIONARY_EXCLUDE'='charField','TABLE_BLOCKSIZE'='512','NO_INVERTED_INDEX'='charField', 'SORT_SCOPE'='GLOBAL_SORT')
       """.stripMargin)

    val storeLocation = s"$rootPath/integration/spark-common-test/src/test/resources/bool/supportBoolean.csv"
    sql(
      s"""
         | LOAD DATA LOCAL INPATH '${storeLocation}'
         | INTO TABLE boolean_table
         | options('FILEHEADER'='shortField,booleanField,intField,bigintField,doubleField,stringField,timestampField,decimalField,dateField,charField,floatField,complexData,booleanField2')
           """.stripMargin)

    checkAnswer(
      sql("select booleanField,intField from boolean_table"),
      Seq(Row(true, 10), Row(false, 17), Row(false, 11),
        Row(true, 10), Row(true, 10), Row(true, 14),
        Row(false, 10), Row(false, 10), Row(false, 16), Row(false, 10))
    )

    checkAnswer(sql("select booleanField from boolean_table where booleanField = true"),
      Seq(Row(true), Row(true), Row(true), Row(true)))

    checkAnswer(sql("select count(*) from boolean_table where booleanField = true"),
      Row(4))

    checkAnswer(sql("select count(*) from boolean_table where booleanField = 'true'"),
      Row(0))

    checkAnswer(sql(
      s"""
         |select count(*)
         |from boolean_table where booleanField = \"true\"
         |""".stripMargin),
      Row(0))

    checkAnswer(sql("select booleanField from boolean_table where booleanField = false"),
      Seq(Row(false), Row(false), Row(false), Row(false), Row(false), Row(false)))

    checkAnswer(sql("select count(*) from boolean_table where booleanField = false"),
      Row(6))

    checkAnswer(sql("select count(*) from boolean_table where booleanField = 'false'"),
      Row(0))

    checkAnswer(sql("select count(*) from boolean_table where booleanField = null"),
      Row(0))

    checkAnswer(sql("select count(*) from boolean_table where booleanField = false or booleanField = true"),
      Row(10))
  }

  test("Loading table: load with DELIMITER, QUOTECHAR, COMMENTCHAR, MULTILINE, ESCAPECHAR, COMPLEX_DELIMITER_LEVEL_1, SINGLE_PASS") {
    sql(
      s"""
         | CREATE TABLE boolean_table(
         | shortField SHORT,
         | booleanField BOOLEAN,
         | intField INT,
         | bigintField LONG,
         | doubleField DOUBLE,
         | stringField STRING,
         | timestampField TIMESTAMP,
         | decimalField DECIMAL(18,2),
         | dateField DATE,
         | charField CHAR(5),
         | floatField FLOAT,
         | complexData ARRAY<STRING>
         | )
         | STORED BY 'carbondata'
         | TBLPROPERTIES('sort_columns'='','DICTIONARY_EXCLUDE'='charField','TABLE_BLOCKSIZE'='512','NO_INVERTED_INDEX'='charField', 'SORT_SCOPE'='GLOBAL_SORT')
       """.stripMargin)

    val storeLocation = s"$rootPath/integration/spark-common-test/src/test/resources/bool/supportBooleanWithFileHeader.csv"
    sql(
      s"""
         | LOAD DATA LOCAL INPATH '${storeLocation}'
         | INTO TABLE boolean_table
         | options('DELIMITER'=',','QUOTECHAR'='"','COMMENTCHAR'='#','MULTILINE'='true','ESCAPECHAR'='\','COMPLEX_DELIMITER_LEVEL_1'='#','COMPLEX_DELIMITER_LEVEL_2'=':','SINGLE_PASS'='TRUE')
           """.stripMargin)

    checkAnswer(
      sql("select booleanField,intField from boolean_table"),
      Seq(Row(true, 10), Row(false, 17), Row(false, 11),
        Row(true, 10), Row(true, 10), Row(true, 14),
        Row(false, 10), Row(false, 10), Row(false, 16), Row(false, 10))
    )

    checkAnswer(sql("select booleanField from boolean_table where booleanField = true"),
      Seq(Row(true), Row(true), Row(true), Row(true)))

    checkAnswer(sql("select count(*) from boolean_table where booleanField = true"),
      Row(4))

    checkAnswer(sql("select count(*) from boolean_table where booleanField = 'true'"),
      Row(0))

    checkAnswer(sql(
      s"""
         |select count(*)
         |from boolean_table where booleanField = \"true\"
         |""".stripMargin),
      Row(0))

    checkAnswer(sql("select booleanField from boolean_table where booleanField = false"),
      Seq(Row(false), Row(false), Row(false), Row(false), Row(false), Row(false)))

    checkAnswer(sql("select count(*) from boolean_table where booleanField = false"),
      Row(6))

    checkAnswer(sql("select count(*) from boolean_table where booleanField = 'false'"),
      Row(0))

    checkAnswer(sql("select count(*) from boolean_table where booleanField = null"),
      Row(0))

    checkAnswer(sql("select count(*) from boolean_table where booleanField = false or booleanField = true"),
      Row(10))
  }

  test("Loading table: bad_records_action is FORCE") {
    val fileLocation = s"$rootPath/integration/spark-common-test/src/test/resources/bool/supportBooleanDifferentFormat.csv"
    sql(
      s"""
         | LOAD DATA LOCAL INPATH '$fileLocation'
         | INTO TABLE carbon_table
         | OPTIONS('FILEHEADER' = 'booleanField','bad_records_logger_enable'='true','bad_records_action'='FORCE')
       """.stripMargin)

    checkAnswer(sql("select * from carbon_table where booleanField = true"),
      Seq(Row(true), Row(true), Row(true), Row(true)))

    checkAnswer(sql("select * from carbon_table"),
      Seq(Row(true), Row(true), Row(true), Row(true),
        Row(false), Row(false), Row(false), Row(false),
        Row(null), Row(null), Row(null), Row(null), Row(null), Row(null),
        Row(null), Row(null), Row(null), Row(null), Row(null), Row(null)))
  }

  ignore("Loading table: bad_records_action is FAIL") {
    val fileLocation = s"$rootPath/integration/spark-common-test/src/test/resources/bool/supportBooleanDifferentFormat.csv"
    sql(
      s"""
         | LOAD DATA LOCAL INPATH '$fileLocation'
         | INTO TABLE carbon_table
         | OPTIONS('FILEHEADER' = 'booleanField','bad_records_logger_enable'='true','bad_records_action'='IGNORE')
       """.stripMargin)

    sql("select * from carbon_table").show()
    checkAnswer(sql("select * from carbon_table where booleanField = true"),
      Seq(Row(true), Row(true), Row(true), Row(true)))

    checkAnswer(sql("select * from carbon_table"),
      Seq(Row(true), Row(true), Row(true), Row(true),
        Row(false), Row(false), Row(false), Row(false),
        Row(null), Row(null), Row(null), Row(null), Row(null), Row(null),
        Row(null), Row(null), Row(null), Row(null), Row(null), Row(null)))
  }


  test("Loading overwrite: into and then overwrite table with another table: support boolean data type and other format") {
    sql(
      s"""
         | CREATE TABLE boolean_table(
         | shortField SHORT,
         | booleanField BOOLEAN,
         | intField INT,
         | bigintField LONG,
         | doubleField DOUBLE,
         | stringField STRING,
         | timestampField TIMESTAMP,
         | decimalField DECIMAL(18,2),
         | dateField DATE,
         | charField CHAR(5),
         | floatField FLOAT,
         | complexData ARRAY<STRING>,
         | booleanField2 BOOLEAN
         | )
         | STORED BY 'carbondata'
         | TBLPROPERTIES('sort_columns'='','DICTIONARY_INCLUDE'='dateField, charField')
       """.stripMargin)

    sql(
      s"""
         | CREATE TABLE boolean_table2(
         | shortField SHORT,
         | booleanField BOOLEAN,
         | intField INT,
         | bigintField LONG,
         | doubleField DOUBLE,
         | stringField STRING,
         | timestampField TIMESTAMP,
         | decimalField DECIMAL(18,2),
         | dateField DATE,
         | charField CHAR(5),
         | floatField FLOAT,
         | complexData ARRAY<STRING>,
         | booleanField2 BOOLEAN
         | )
         | STORED BY 'carbondata'
         | TBLPROPERTIES('sort_columns'='','DICTIONARY_INCLUDE'='dateField, charField')
       """.stripMargin)

    sql(
      s"""
         | CREATE TABLE boolean_table3(
         | shortField SHORT,
         | booleanField BOOLEAN,
         | intField INT,
         | stringField STRING,
         | booleanField2 BOOLEAN
         | )
         | STORED BY 'carbondata'
         | TBLPROPERTIES('sort_columns'='')
       """.stripMargin)
    sql(
      s"""
         | CREATE TABLE boolean_table4(
         | shortField SHORT,
         | booleanField BOOLEAN,
         | intField INT,
         | stringField STRING,
         | booleanField2 BOOLEAN
         | )
         | STORED BY 'carbondata'
         | TBLPROPERTIES('sort_columns'='')
       """.stripMargin)

    val rootPath = new File(this.getClass.getResource("/").getPath
      + "../../../..").getCanonicalPath
    val storeLocation = s"$rootPath/integration/spark-common-test/src/test/resources/bool/supportBooleanTwoBooleanColumns.csv"

    sql(
      s"""
         | LOAD DATA LOCAL INPATH '${storeLocation}'
         | INTO TABLE boolean_table
         | options('FILEHEADER'='shortField,booleanField,intField,bigintField,doubleField,stringField,timestampField,decimalField,dateField,charField,floatField,complexData,booleanField2')
           """.stripMargin)

    sql(
      s"""
         | LOAD DATA LOCAL INPATH '${storeLocation}'
         | overwrite INTO TABLE boolean_table
         | options('FILEHEADER'='shortField,booleanField,intField,bigintField,doubleField,stringField,timestampField,decimalField,dateField,charField,floatField,complexData,booleanField2')
           """.stripMargin)

    sql("insert overwrite table boolean_table2 select * from boolean_table")
    sql("insert overwrite table boolean_table3 select shortField,booleanField,intField,stringField,booleanField2 from boolean_table")
    sql("insert overwrite table boolean_table4 select shortField,booleanField,intField,stringField,booleanField2 from boolean_table where shortField > 3")

    checkAnswer(
      sql("select booleanField,intField from boolean_table2"),
      Seq(Row(true, 10), Row(false, 17), Row(false, 11),
        Row(true, 10), Row(true, 10), Row(true, 14),
        Row(false, 10), Row(false, 10), Row(false, 16), Row(false, 10))
    )

    checkAnswer(
      sql("select booleanField,intField from boolean_table3"),
      Seq(Row(true, 10), Row(false, 17), Row(false, 11),
        Row(true, 10), Row(true, 10), Row(true, 14),
        Row(false, 10), Row(false, 10), Row(false, 16), Row(false, 10))
    )

    checkAnswer(
      sql("select booleanField,intField from boolean_table4"),
      Seq(Row(false, 17), Row(false, 16))
    )

    checkAnswer(
      sql("select booleanField,intField,booleanField2 from boolean_table2"),
      Seq(Row(true, 10, true), Row(false, 17, true), Row(false, 11, true),
        Row(true, 10, true), Row(true, 10, true), Row(true, 14, false),
        Row(false, 10, false), Row(false, 10, false), Row(false, 16, false), Row(false, 10, false))
    )

    checkAnswer(
      sql("select booleanField,intField,booleanField2 from boolean_table3"),
      Seq(Row(true, 10, true), Row(false, 17, true), Row(false, 11, true),
        Row(true, 10, true), Row(true, 10, true), Row(true, 14, false),
        Row(false, 10, false), Row(false, 10, false), Row(false, 16, false), Row(false, 10, false))
    )
  }

  test("Loading overwrite: support boolean data type format, different format") {
    val fileLocation = s"$rootPath/integration/spark-common-test/src/test/resources/bool/supportBooleanDifferentFormat.csv"
    sql(
      s"""
         | LOAD DATA LOCAL INPATH '$fileLocation'
         | INTO TABLE carbon_table
         | OPTIONS('FILEHEADER' = 'booleanField')
       """.stripMargin)

    sql(
      s"""
         | LOAD DATA LOCAL INPATH '$fileLocation'
         | OVERWRITE INTO TABLE carbon_table
         | OPTIONS('FILEHEADER' = 'booleanField')
       """.stripMargin)


    checkAnswer(sql("select * from carbon_table where booleanField = true"),
      Seq(Row(true), Row(true), Row(true), Row(true)))

    checkAnswer(sql("select * from carbon_table"),
      Seq(Row(true), Row(true), Row(true), Row(true), Row(false), Row(false), Row(false), Row(false)
        , Row(null), Row(null), Row(null), Row(null), Row(null), Row(null), Row(null), Row(null), Row(null), Row(null), Row(null), Row(null)))
  }

  test("Loading overwrite: support boolean and other data type") {
    sql(
      s"""
         | CREATE TABLE boolean_table(
         | shortField SHORT,
         | booleanField BOOLEAN,
         | intField INT,
         | bigintField LONG,
         | doubleField DOUBLE,
         | stringField STRING,
         | timestampField TIMESTAMP,
         | decimalField DECIMAL(18,2),
         | dateField DATE,
         | charField CHAR(5),
         | floatField FLOAT,
         | complexData ARRAY<STRING>,
         | booleanField2 BOOLEAN
         | )
         | STORED BY 'carbondata'
         | TBLPROPERTIES('sort_columns'='','DICTIONARY_INCLUDE'='dateField, charField')
       """.stripMargin)

    val storeLocation = s"$rootPath/integration/spark-common-test/src/test/resources/bool/supportBoolean.csv"
    sql(
      s"""
         | LOAD DATA LOCAL INPATH '${storeLocation}'
         | INTO TABLE boolean_table
         | options('FILEHEADER'='shortField,booleanField,intField,bigintField,doubleField,stringField,timestampField,decimalField,dateField,charField,floatField,complexData,booleanField2')
           """.stripMargin)
    sql(
      s"""
         | LOAD DATA LOCAL INPATH '${storeLocation}'
         | OVERWRITE INTO TABLE boolean_table
         | options('FILEHEADER'='shortField,booleanField,intField,bigintField,doubleField,stringField,timestampField,decimalField,dateField,charField,floatField,complexData,booleanField2')
           """.stripMargin)
    checkAnswer(
      sql("select booleanField,intField from boolean_table"),
      Seq(Row(true, 10), Row(false, 17), Row(false, 11),
        Row(true, 10), Row(true, 10), Row(true, 14),
        Row(false, 10), Row(false, 10), Row(false, 16), Row(false, 10))
    )
  }

  test("Comparing table: support boolean and other data type") {
    sql(
      s"""
         | CREATE TABLE boolean_table(
         | shortField SHORT,
         | booleanField BOOLEAN,
         | intField INT,
         | bigintField LONG,
         | doubleField DOUBLE,
         | stringField STRING,
         | timestampField TIMESTAMP,
         | decimalField DECIMAL(18,2),
         | dateField DATE,
         | charField CHAR(5),
         | floatField FLOAT,
         | complexData ARRAY<STRING>,
         | booleanField2 BOOLEAN
         | )
         | STORED BY 'carbondata'
         | TBLPROPERTIES('sort_columns'='','DICTIONARY_INCLUDE'='dateField, charField')
       """.stripMargin)

    sql(
      s"""
         | CREATE TABLE boolean_table2(
         | shortField SHORT,
         | booleanField BOOLEAN,
         | intField INT,
         | bigintField LONG,
         | doubleField DOUBLE,
         | stringField STRING,
         | timestampField TIMESTAMP,
         | decimalField DECIMAL(18,2),
         | dateField DATE,
         | charField CHAR(5),
         | floatField FLOAT,
         | complexData ARRAY<STRING>,
         | booleanField2 BOOLEAN
         | )
         | STORED BY 'carbondata'
         | TBLPROPERTIES('sort_columns'='','DICTIONARY_INCLUDE'='dateField, charField')
       """.stripMargin)

    val storeLocation = s"$rootPath/integration/spark-common-test/src/test/resources/bool/supportBooleanTwoBooleanColumns.csv"
    sql(
      s"""
         | LOAD DATA LOCAL INPATH '${storeLocation}'
         | INTO TABLE boolean_table
         | options('FILEHEADER'='shortField,booleanField,intField,bigintField,doubleField,stringField,timestampField,decimalField,dateField,charField,floatField,complexData,booleanField2')
           """.stripMargin)

    sql("insert into boolean_table2 select * from boolean_table where shortField = 1 and booleanField = true")

    checkAnswer(
      sql("select booleanField,intField,booleanField2 from boolean_table"),
      Seq(Row(true, 10, true), Row(false, 17, true), Row(false, 11, true),
        Row(true, 10, true), Row(true, 10, true), Row(true, 14, false),
        Row(false, 10, false), Row(false, 10, false), Row(false, 16, false), Row(false, 10, false))
    )

    checkAnswer(
      sql("select booleanField,intField,booleanField2 from boolean_table"),
      Seq(Row(true, 10, true), Row(false, 17, true), Row(false, 11, true),
        Row(true, 10, true), Row(true, 10, true), Row(true, 14, false),
        Row(false, 10, false), Row(false, 10, false), Row(false, 16, false), Row(false, 10, false))
    )

    checkAnswer(
      sql("select booleanField,intField,booleanField2 from boolean_table2"),
      Seq(Row(true, 10, true), Row(true, 10, true), Row(true, 10, true))
    )

    checkAnswer(
      sql("select booleanField,intField,booleanField2 from boolean_table where exists (select booleanField,intField,booleanField2 " +
        "from boolean_table2 where boolean_table.intField=boolean_table2.intField)"),
      Seq(Row(true, 10, true), Row(true, 10, true), Row(true, 10, true), Row(false, 10, false), Row(false, 10, false), Row(false, 10, false))
    )
  }
}
