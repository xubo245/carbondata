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
package org.apache.spark.sql.carbondata.datasource

import java.io.File

import org.apache.carbondata.core.constants.CarbonCommonConstants
import org.apache.carbondata.core.util.CarbonProperties
import org.apache.carbondata.test.util.BinaryUtil
import org.apache.commons.io.FileUtils

import org.apache.spark.sql.Row
import org.apache.spark.sql.carbondata.datasource.TestUtil._
import org.apache.spark.util.SparkUtil

import org.scalatest.{BeforeAndAfterAll, FunSuite}

class SparkCarbonDataSourceBinaryTest extends FunSuite with BeforeAndAfterAll {

    var writerPath = new File(this.getClass.getResource("/").getPath
            + "../../target/SparkCarbonFileFormat/WriterOutput/")
            .getCanonicalPath
    var outputPath = writerPath + 2
    //getCanonicalPath gives path with \, but the code expects /.
    writerPath = writerPath.replace("\\", "/")

    var sdkPath = new File(this.getClass.getResource("/").getPath + "../../../../store/sdk/")
            .getCanonicalPath

    def buildTestBinaryData(): Any = {
        FileUtils.deleteDirectory(new File(writerPath))

        val sourceImageFolder = sdkPath + "/src/test/resources/image/flowers"
        val sufAnnotation = ".txt"
        BinaryUtil.binaryToCarbon(sourceImageFolder, writerPath, sufAnnotation, ".jpg")
    }

    def cleanTestData() = {
        FileUtils.deleteDirectory(new File(writerPath))
        FileUtils.deleteDirectory(new File(outputPath))
    }

    import spark._

    override def beforeAll(): Unit = {
        CarbonProperties.getInstance()
                .addProperty(CarbonCommonConstants.CARBON_TIMESTAMP_FORMAT,
                    CarbonCommonConstants.CARBON_TIMESTAMP_DEFAULT_FORMAT)
        buildTestBinaryData()

        FileUtils.deleteDirectory(new File(outputPath))
        sql("DROP TABLE IF EXISTS sdkOutputTable")
    }

    override def afterAll(): Unit = {
        cleanTestData()
        sql("DROP TABLE IF EXISTS sdkOutputTable")
    }

    test("Test direct sql read carbon") {
        assert(new File(writerPath).exists())
        checkAnswer(
            sql(s"SELECT COUNT(*) FROM carbon.`$writerPath`"),
            Seq(Row(3)))
    }

    test("Test read image carbon with spark carbon file format, generate by sdk, CTAS") {
        sql("DROP TABLE IF EXISTS binaryCarbon")
        sql("DROP TABLE IF EXISTS binaryCarbon3")
        if (SparkUtil.isSparkVersionEqualTo("2.1")) {
            sql(s"CREATE TABLE binaryCarbon USING CARBON OPTIONS(PATH '$writerPath')")
            sql(s"CREATE TABLE binaryCarbon3 USING CARBON OPTIONS(PATH '$outputPath')" + " AS SELECT * FROM binaryCarbon")
        } else {
            sql(s"CREATE TABLE binaryCarbon USING CARBON LOCATION '$writerPath'")
            sql(s"CREATE TABLE binaryCarbon3 USING CARBON LOCATION '$outputPath'" + " AS SELECT * FROM binaryCarbon")
        }
        checkAnswer(sql("SELECT COUNT(*) FROM binaryCarbon"),
            Seq(Row(3)))
        checkAnswer(sql("SELECT COUNT(*) FROM binaryCarbon3"),
            Seq(Row(3)))
        sql("DROP TABLE IF EXISTS binaryCarbon")
        sql("DROP TABLE IF EXISTS binaryCarbon3")
    }

    test("Don't support sort_columns") {
        import spark._
        sql("DROP TABLE IF EXISTS binaryTable")
        val exception = intercept[Exception] {
            sql(
                s"""
                   | CREATE TABLE binaryTable (
                   |    id DOUBLE,
                   |    label BOOLEAN,
                   |    name STRING,
                   |    image BINARY,
                   |    autoLabel BOOLEAN)
                   | using carbon
                   | options('SORT_COLUMNS'='image')
       """.stripMargin)
            sql("SELECT COUNT(*) FROM binaryTable").show()
        }
        assert(exception.getCause.getMessage.contains("sort columns not supported for array, struct, map, double, float, decimal, varchar, binary"))
    }

    test("Don't support long_string_columns for binary") {
        import spark._
        sql("DROP TABLE IF EXISTS binaryTable")
        val exception = intercept[Exception] {
            sql(
                s"""
                   | CREATE TABLE binaryTable (
                   |    id DOUBLE,
                   |    label BOOLEAN,
                   |    name STRING,
                   |    image BINARY,
                   |    autoLabel BOOLEAN)
                   | using carbon
                   | options('long_string_columns'='image')
       """.stripMargin)
            sql("SELECT COUNT(*) FROM binaryTable").show()
        }
        assert(exception.getCause.getMessage.contains("long string column : image is not supported for data type: BINARY"))
    }

    test("Desc formatted fot binary column") {
        sql("DROP TABLE IF EXISTS binaryCarbon")
        sql("DROP TABLE IF EXISTS binaryCarbon3")
        if (SparkUtil.isSparkVersionXandAbove("2.2")) {
            sql(s"CREATE TABLE binaryCarbon USING CARBON LOCATION '$writerPath'")
            sql(s"CREATE TABLE binaryCarbon3 USING CARBON LOCATION '$outputPath'" + " AS SELECT * FROM binaryCarbon")
        }
        val result = sql("desc formatted binaryCarbon").collect()
        var flag = false
        result.foreach { each =>
            print(each)
            if ("binary".equals(each.get(1))) {
                flag = true
            }
        }
        assert(flag)
        checkAnswer(sql("SELECT COUNT(*) FROM binaryCarbon"),
            Seq(Row(3)))
        checkAnswer(sql("SELECT COUNT(*) FROM binaryCarbon3"),
            Seq(Row(3)))
        sql("DROP TABLE IF EXISTS binaryCarbon")
        sql("DROP TABLE IF EXISTS binaryCarbon3")
    }

    test("Don't support load for datasource") {
        sql("DROP TABLE IF EXISTS binaryCarbon")
        if (SparkUtil.isSparkVersionXandAbove("2.2")) {
            sql(
                s"""
                   | CREATE TABLE binaryCarbon(
                   |    binaryId INT,
                   |    binaryName STRING,
                   |    binary BINARY,
                   |    labelName STRING,
                   |    labelContent STRING
                   |) USING CARBON  """.stripMargin)

            val exception = intercept[Exception] {
                sql(s"load data local inpath '$writerPath' into table binaryCarbon")
            }
            assert(exception.getMessage.contains("LOAD DATA is not supported for datasource tables"))
        }
        sql("DROP TABLE IF EXISTS binaryCarbon")
    }

    //TODO: support insert into
    test("Test partition") {
        sql("DROP TABLE IF EXISTS binaryCarbon")
        sql("DROP TABLE IF EXISTS binaryCarbon3")
        if (SparkUtil.isSparkVersionXandAbove("2.2")) {
            sql(s"CREATE TABLE binaryCarbon USING CARBON LOCATION '$writerPath'")
            sql(
                s"""
                   | CREATE TABLE binaryCarbon3(
                   |    binaryId INT,
                   |    binaryName STRING,
                   |    binary BINARY,
                   |    labelName STRING,
                   |    labelContent STRING
                   |) USING CARBON partitioned by (binary) """.stripMargin)
            sql("select * from binaryCarbon  where binaryId=0 ").show()
            try {
                sql("insert into binaryCarbon3 select binaryId,binaryName,binary,labelName,labelContent from binaryCarbon where binaryId=0 ")
                assert(false)
            } catch {
                case e: Exception =>
                    e.printStackTrace()
                    assert(true)
            }
        }
        sql("DROP TABLE IF EXISTS binaryCarbon")
        sql("DROP TABLE IF EXISTS binaryCarbon3")
    }

    test("Test unsafe as false") {
        CarbonProperties.getInstance()
                .addProperty(CarbonCommonConstants.ENABLE_UNSAFE_COLUMN_PAGE, "false")
        sql("DROP TABLE IF EXISTS binaryCarbon")
        sql("DROP TABLE IF EXISTS binaryCarbon3")
        if (SparkUtil.isSparkVersionEqualTo("2.1")) {
            sql(s"CREATE TABLE binaryCarbon USING CARBON OPTIONS(PATH '$writerPath')")
            sql(s"CREATE TABLE binaryCarbon3 USING CARBON OPTIONS(PATH '$outputPath')" + " AS SELECT * FROM binaryCarbon")
        } else {
            sql(s"CREATE TABLE binaryCarbon USING CARBON LOCATION '$writerPath'")
            sql(s"CREATE TABLE binaryCarbon3 USING CARBON LOCATION '$outputPath'" + " AS SELECT * FROM binaryCarbon")
        }
        checkAnswer(sql("SELECT COUNT(*) FROM binaryCarbon"),
            Seq(Row(3)))
        checkAnswer(sql("SELECT COUNT(*) FROM binaryCarbon3"),
            Seq(Row(3)))
        sql("DROP TABLE IF EXISTS binaryCarbon")
        sql("DROP TABLE IF EXISTS binaryCarbon3")

        CarbonProperties.getInstance()
                .addProperty(CarbonCommonConstants.ENABLE_UNSAFE_COLUMN_PAGE,
                    CarbonCommonConstants.ENABLE_UNSAFE_COLUMN_PAGE_DEFAULT)
    }
}
