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

package org.apache.carbondata.spark.testsuite.createTable

import java.io._

import org.apache.carbondata.core.constants.CarbonCommonConstants
import org.apache.carbondata.core.util.CarbonProperties
import org.apache.carbondata.util.ImageUtil

import org.apache.commons.io.FileUtils
import org.apache.spark.sql.Row
import org.apache.spark.sql.test.util.QueryTest
import org.apache.spark.util.SparkUtil

import org.scalatest.BeforeAndAfterAll


class TestNonTransactionalCarbonTableForBinary extends QueryTest with BeforeAndAfterAll {

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
        val imageUtil = new ImageUtil()

        val sourceImageFolder = sdkPath + "/src/main/resources/image/flowers"
        val sufAnnotation = ".txt"
        imageUtil.writeAndRead(sourceImageFolder, writerPath, sufAnnotation, ".jpg")
    }


    def cleanTestData() = {
        FileUtils.deleteDirectory(new File(writerPath))
        FileUtils.deleteDirectory(new File(outputPath))
    }

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

    test("test read image carbon with external table, generate by sdk, CTAS") {
        sql("DROP TABLE IF EXISTS binaryCarbon")
        sql("DROP TABLE IF EXISTS binaryCarbon3")
        if (SparkUtil.isSparkVersionEqualTo("2.1")) {
            sql(s"CREATE EXTERNAL TABLE binaryCarbon STORED BY 'carbondata' OPTIONS(PATH '$writerPath')")
            val exception = intercept[Exception] {
                sql(s"CREATE TABLE binaryCarbon3 STORED BY 'carbondata' OPTIONS(PATH '$outputPath')" + " AS SELECT * FROM binaryCarbon")
            }
            assert(exception.getMessage.contains("DataLoad failure: Error while initializing data handler"))
        } else {
            sql(s"CREATE EXTERNAL TABLE binaryCarbon STORED BY 'carbondata'LOCATION '$writerPath'")
            val exception = intercept[Exception] {
                sql(s"CREATE TABLE binaryCarbon3 STORED BY 'carbondata' LOCATION '$outputPath'" + " AS SELECT * FROM binaryCarbon")
            }
            assert(exception.getMessage.contains("DataLoad failure: Error while initializing data handler : Failed for table"))
        }
        checkAnswer(sql("SELECT COUNT(*) FROM binaryCarbon"),
            Seq(Row(3)))
        sql("DROP TABLE IF EXISTS binaryCarbon")
        sql("DROP TABLE IF EXISTS binaryCarbon3")
    }

}
