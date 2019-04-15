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
package org.apache.carbondata.integration.spark.testsuite.binary

import java.util.Arrays

import org.apache.carbondata.common.exceptions.sql.MalformedCarbonCommandException
import org.apache.carbondata.core.constants.CarbonCommonConstants
import org.apache.carbondata.core.metadata.CarbonMetadata
import org.apache.carbondata.core.metadata.schema.table.CarbonTable
import org.apache.carbondata.core.util.CarbonProperties

import org.apache.spark.sql.Row
import org.apache.spark.sql.test.util.QueryTest
import org.scalatest.BeforeAndAfterAll

/**
  * Test cases for testing binary
  */
class TestBinaryDataType extends QueryTest with BeforeAndAfterAll {
    override def beforeAll {
    }

    test("Create table and load data with binary column") {
        sql("DROP TABLE IF EXISTS binaryTable")
        sql(
            s"""
               | CREATE TABLE IF NOT EXISTS binaryTable (
               |    id int,
               |    label boolean,
               |    name string,
               |    image binary,
               |    autoLabel boolean)
               | STORED BY 'carbondata'
             """.stripMargin)
        sql(
            s"""
               | LOAD DATA LOCAL INPATH '$resourcesPath/binarydata.csv'
               | INTO TABLE binaryTable
               | OPTIONS('header'='false')
             """.stripMargin)
        checkAnswer(sql("SELECT COUNT(*) FROM binaryTable"), Seq(Row(3)))
        try {
            val df = sql("SELECT * FROM binaryTable").collect()
            assert(3 == df.length)
            df.foreach { each =>
                assert(5 == each.length)

                assert(Integer.valueOf(each(0).toString) > 0)
                assert(each(1).toString.equalsIgnoreCase("false") || (each(1).toString.equalsIgnoreCase("true")))
                assert(each(2).toString.contains(".png"))

                val bytes20 = each.getAs[Array[Byte]](3).slice(0, 20)
                val binaryName = each(2).toString
                val expectedBytes = firstBytes20.get(binaryName).get
                assert(Arrays.equals(expectedBytes, bytes20), "incorrect numeric value for flattened image")

                assert(each(4).toString.equalsIgnoreCase("false") || (each(4).toString.equalsIgnoreCase("true")))
            }
        } catch {
            case e: Exception =>
                e.printStackTrace()
                assert(false)
        }
    }

    test("Support projection for binary") {
        sql("DROP TABLE IF EXISTS binaryTable")
        sql(
            s"""
               | CREATE TABLE IF NOT EXISTS binaryTable (
               |    id int,
               |    label boolean,
               |    name string,
               |    image binary,
               |    autoLabel boolean)
               | STORED BY 'carbondata'
             """.stripMargin)
        sql(
            s"""
               | LOAD DATA LOCAL INPATH '$resourcesPath/binarydata.csv'
               | INTO TABLE binaryTable
               | OPTIONS('header'='false')
             """.stripMargin)
        checkAnswer(sql("SELECT COUNT(*) FROM binaryTable"), Seq(Row(3)))
        try {
            val df = sql("SELECT name,image FROM binaryTable").collect()
            assert(3 == df.length)
            df.foreach { each =>
                assert(2 == each.length)
                val binaryName = each(0).toString
                val bytes20 = each.getAs[Array[Byte]](1).slice(0, 20)
                val expectedBytes = firstBytes20.get(binaryName).get
                assert(Arrays.equals(expectedBytes, bytes20), "incorrect numeric value for flattened image")
            }
        } catch {
            case e: Exception =>
                e.printStackTrace()
                assert(false)
        }
    }

    private val firstBytes20 = Map("1.png" -> Array[Byte](-119, 80, 78, 71, 13, 10, 26, 10, 0, 0, 0, 13, 73, 72, 68, 82, 0, 0, 1, 74),
        "2.png" -> Array[Byte](-119, 80, 78, 71, 13, 10, 26, 10, 0, 0, 0, 13, 73, 72, 68, 82, 0, 0, 2, -11),
        "3.png" -> Array[Byte](-119, 80, 78, 71, 13, 10, 26, 10, 0, 0, 0, 13, 73, 72, 68, 82, 0, 0, 1, 54)
    )

    test("Create no sort table and load data with binary column") {
        sql("DROP TABLE IF EXISTS binaryTable")
        sql(
            s"""
               | CREATE TABLE IF NOT EXISTS binaryTable (
               |    id INT,
               |    label boolean,
               |    name STRING,
               |    image BINARY,
               |    autoLabel boolean)
               | STORED BY 'carbondata'
               | TBLPROPERTIES('SORT_COLUMNS'='')
             """.stripMargin)
        sql(
            s"""
               | LOAD DATA LOCAL INPATH '$resourcesPath/binarydata.csv'
               | INTO TABLE binaryTable
               | OPTIONS('header'='false')
             """.stripMargin)
        checkAnswer(sql("SELECT COUNT(*) FROM binaryTable"), Seq(Row(3)))
    }

    test("Don't support sort_columns") {
        sql("DROP TABLE IF EXISTS binaryTable")
        val exception = intercept[Exception] {
            sql(
                s"""
                   | CREATE TABLE IF NOT EXISTS binaryTable (
                   |    id double,
                   |    label boolean,
                   |    name STRING,
                   |    image BINARY,
                   |    autoLabel boolean)
                   | STORED BY 'carbondata'
                   | TBLPROPERTIES('SORT_COLUMNS'='image')
             """.stripMargin)
        }
        assert(exception.getMessage.contains("sort_columns is unsupported for binary datatype column"))
    }

    test("Unsupport LOCAL_DICTIONARY_INCLUDE for binary") {

        sql("DROP TABLE IF EXISTS binaryTable")
        val exception = intercept[MalformedCarbonCommandException] {
            sql(
                """
                  | CREATE TABLE binaryTable(
                  |     id int,
                  |     name string,
                  |     city string,
                  |     age int,
                  |     image binary)
                  | STORED BY 'org.apache.carbondata.format'
                  | tblproperties('local_dictionary_enable'='true','local_dictionary_include'='image')
                """.stripMargin)
        }
        assert(exception.getMessage.contains(
            "LOCAL_DICTIONARY_INCLUDE/LOCAL_DICTIONARY_EXCLUDE column: image is not a string/complex/varchar datatype column. " +
                    "LOCAL_DICTIONARY_COLUMN should be no dictionary string/complex/varchar datatype column"))
    }

    test("COLUMN_META_CACHE for binary") {
        sql("DROP TABLE IF EXISTS binaryTable")
        sql(
            s"""
               | CREATE TABLE IF NOT EXISTS binaryTable (
               |    id INT,
               |    label boolean,
               |    name STRING,
               |    image BINARY,
               |    autoLabel boolean)
               | STORED BY 'carbondata'
               | TBLPROPERTIES('COLUMN_META_CACHE'='image')
             """.stripMargin)
        sql(
            s"""
               | LOAD DATA LOCAL INPATH '$resourcesPath/binarydata.csv'
               | INTO TABLE binaryTable
               | OPTIONS('header'='false')
             """.stripMargin)
        checkAnswer(sql("SELECT COUNT(*) FROM binaryTable"), Seq(Row(3)))
    }

    // TODO: check the result
    test("RANGE_COLUMN for binary") {
        sql("DROP TABLE IF EXISTS binaryTable")
        sql(
            s"""
               | CREATE TABLE IF NOT EXISTS binaryTable (
               |    id INT,
               |    label boolean,
               |    name STRING,
               |    image BINARY,
               |    autoLabel boolean)
               | STORED BY 'carbondata'
               | TBLPROPERTIES('RANGE_COLUMN'='image')
             """.stripMargin)
        sql(
            s"""
               | LOAD DATA LOCAL INPATH '$resourcesPath/binarydata.csv'
               | INTO TABLE binaryTable
               | OPTIONS('header'='false','global_sort_partitions'='2')
             """.stripMargin)
        checkAnswer(sql("SELECT COUNT(*) FROM binaryTable"), Seq(Row(3)))
    }

    test("Test carbon.column.compressor=zstd") {
        sql("DROP TABLE IF EXISTS binaryTable")
        sql(
            s"""
               | CREATE TABLE IF NOT EXISTS binaryTable (
               |    id INT,
               |    label boolean,
               |    name STRING,
               |    image BINARY,
               |    autoLabel boolean)
               | STORED BY 'carbondata'
               | TBLPROPERTIES('carbon.column.compressor'='zstd')
             """.stripMargin)
        sql(
            s"""
               | LOAD DATA LOCAL INPATH '$resourcesPath/binarydata.csv'
               | INTO TABLE binaryTable
               | OPTIONS('header'='false')
             """.stripMargin)
        checkAnswer(sql("SELECT COUNT(*) FROM binaryTable"), Seq(Row(3)))
    }

    test("Support filter other column in binary table") {
        sql("DROP TABLE IF EXISTS binaryTable")
        sql(
            s"""
               | CREATE TABLE IF NOT EXISTS binaryTable (
               |    id INT,
               |    label boolean,
               |    name STRING,
               |    image BINARY,
               |    autoLabel boolean)
               | STORED BY 'carbondata'
               | TBLPROPERTIES('carbon.column.compressor'='zstd')
             """.stripMargin)
        sql(
            s"""
               | LOAD DATA LOCAL INPATH '$resourcesPath/binarydata.csv'
               | INTO TABLE binaryTable
               | OPTIONS('header'='false')
             """.stripMargin)
        checkAnswer(sql("SELECT COUNT(*) FROM binaryTable where id =1"), Seq(Row(1)))
    }

    test("Test create table with buckets unsafe") {
        CarbonProperties.getInstance().addProperty(CarbonCommonConstants.ENABLE_UNSAFE_SORT, "true")
        sql("DROP TABLE IF EXISTS binaryTable")
        sql(
            s"""
               | CREATE TABLE IF NOT EXISTS binaryTable (
               |    id INT,
               |    label boolean,
               |    name STRING,
               |    image BINARY,
               |    autoLabel boolean)
               | STORED BY 'carbondata'
               | TBLPROPERTIES('BUCKETNUMBER'='4', 'BUCKETCOLUMNS'='image')
             """.stripMargin)
        sql(
            s"""
               | LOAD DATA LOCAL INPATH '$resourcesPath/binarydata.csv'
               | INTO TABLE binaryTable
               | OPTIONS('header'='false')
             """.stripMargin)

        CarbonProperties.getInstance().addProperty(CarbonCommonConstants.ENABLE_UNSAFE_SORT, "false")
        val table: CarbonTable = CarbonMetadata.getInstance().getCarbonTable("default", "binaryTable")
        if (table != null && table.getBucketingInfo("binarytable") != null) {
            assert(true)
        } else {
            assert(false, "Bucketing info does not exist")
        }
    }

    override def afterAll: Unit = {
        sql("DROP TABLE IF EXISTS binaryTable")
    }
}