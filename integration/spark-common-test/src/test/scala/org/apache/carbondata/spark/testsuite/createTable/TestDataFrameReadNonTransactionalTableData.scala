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
import java.sql.Timestamp
import java.util
import java.util.concurrent.TimeUnit

import scala.collection.JavaConverters._
import scala.collection.mutable
import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.duration.Duration
import scala.concurrent.{Await, Future}
import org.apache.avro
import org.apache.avro.file.DataFileWriter
import org.apache.avro.generic.{GenericDatumReader, GenericDatumWriter, GenericRecord}
import org.apache.avro.io.{DecoderFactory, Encoder}
import org.apache.commons.io.FileUtils
import org.apache.spark.sql.test.util.QueryTest
import org.apache.spark.sql.{CarbonEnv, Row, SaveMode}
import org.junit.Assert
import org.scalatest.BeforeAndAfterAll
import org.apache.carbondata.common.exceptions.sql.MalformedCarbonCommandException
import org.apache.carbondata.core.cache.dictionary.DictionaryByteArrayWrapper
import org.apache.carbondata.core.constants.CarbonCommonConstants
import org.apache.carbondata.core.datastore.block.TableBlockInfo
import org.apache.carbondata.core.datastore.chunk.impl.DimensionRawColumnChunk
import org.apache.carbondata.core.datastore.chunk.reader.CarbonDataReaderFactory
import org.apache.carbondata.core.datastore.chunk.reader.dimension.v3.CompressedDimensionChunkFileBasedReaderV3
import org.apache.carbondata.core.datastore.compression.CompressorFactory
import org.apache.carbondata.core.datastore.filesystem.{CarbonFile, CarbonFileFilter}
import org.apache.carbondata.core.datastore.impl.FileFactory
import org.apache.carbondata.core.datastore.page.encoding.DefaultEncodingFactory
import org.apache.carbondata.core.metadata.ColumnarFormatVersion
import org.apache.carbondata.core.metadata.datatype.DataTypes
import org.apache.carbondata.core.util.{CarbonMetadataUtil, CarbonProperties, CarbonUtil, DataFileFooterConverterV3}
import org.apache.carbondata.processing.loading.exception.CarbonDataLoadingException
import org.apache.carbondata.sdk.file._


class TestDataFrameReadNonTransactionalTableData extends QueryTest with BeforeAndAfterAll {

  var writerPath = new File(this.getClass.getResource("/").getPath
    +
    "../." +
    "./target/SparkCarbonFileFormat/WriterOutput/")
    .getCanonicalPath
  //getCanonicalPath gives path with \, but the code expects /.
  writerPath = writerPath.replace("\\", "/")

  def buildTestDataSingleFile(): Any = {
    FileUtils.deleteDirectory(new File(writerPath))
    buildTestData(3, null)
  }

  def buildTestDataMultipleFiles(): Any = {
    FileUtils.deleteDirectory(new File(writerPath))
    buildTestData(1000000, null)
  }

  def buildTestDataTwice(): Any = {
    FileUtils.deleteDirectory(new File(writerPath))
    buildTestData(3, null)
    buildTestData(3, null)
  }

  def buildTestDataSameDirectory(): Any = {
    buildTestData(3, null)
  }

  def buildTestDataWithBadRecordForce(): Any = {
    FileUtils.deleteDirectory(new File(writerPath))
    var options = Map("bAd_RECords_action" -> "FORCE").asJava
    buildTestData(3, options)
  }

  def buildTestDataWithBadRecordFail(): Any = {
    FileUtils.deleteDirectory(new File(writerPath))
    var options = Map("bAd_RECords_action" -> "FAIL").asJava
    buildTestData(15001, options)
  }

  def buildTestDataWithBadRecordIgnore(): Any = {
    FileUtils.deleteDirectory(new File(writerPath))
    var options = Map("bAd_RECords_action" -> "IGNORE").asJava
    buildTestData(3, options)
  }

  def buildTestDataWithBadRecordRedirect(): Any = {
    FileUtils.deleteDirectory(new File(writerPath))
    var options = Map("bAd_RECords_action" -> "REDIRECT").asJava
    buildTestData(3, options)
  }

  def buildTestDataWithSortColumns(sortColumns: List[String]): Any = {
    FileUtils.deleteDirectory(new File(writerPath))
    buildTestData(3, null, sortColumns)
  }

  def buildTestData(rows: Int, options: util.Map[String, String]): Any = {
    buildTestData(rows, options, List("name"))
  }

  // prepare sdk writer output
  def buildTestData(rows: Int,
                    options: util.Map[String, String],
                    sortColumns: List[String]): Any = {
    val schema = new StringBuilder()
      .append("[ \n")
      .append("   {\"NaMe\":\"string\"},\n")
      .append("   {\"age\":\"int\"},\n")
      .append("   {\"height\":\"double\"}\n")
      .append("]")
      .toString()

    try {
      val builder = CarbonWriter.builder()
      val writer =
        if (options != null) {
          builder.outputPath(writerPath)
            .sortBy(sortColumns.toArray)
            .uniqueIdentifier(
              System.currentTimeMillis).withBlockSize(2).withLoadOptions(options)
            .withCsvInput(Schema.parseJson(schema)).writtenBy("TestNonTransactionalCarbonTable").build()
        } else {
          builder.outputPath(writerPath)
            .sortBy(sortColumns.toArray)
            .uniqueIdentifier(
              System.currentTimeMillis).withBlockSize(2)
            .withCsvInput(Schema.parseJson(schema)).writtenBy("TestNonTransactionalCarbonTable").build()
        }
      var i = 0
      while (i < rows) {
        if ((options != null) && (i < 3)) {
          // writing a bad record
          writer.write(Array[String]("robot" + i, String.valueOf(i.toDouble / 2), "robot"))
        } else {
          writer.write(Array[String]("robot" + i, String.valueOf(i), String.valueOf(i.toDouble / 2)))
        }
        i += 1
      }
      if (options != null) {
        //Keep one valid record. else carbon data file will not generate
        writer.write(Array[String]("robot" + i, String.valueOf(i), String.valueOf(i.toDouble / 2)))
      }
      writer.close()
    } catch {
      case ex: Throwable => throw new RuntimeException(ex)
    }
  }

  // prepare sdk writer output with other schema
  def buildTestDataOtherDataType(rows: Int, sortColumns: Array[String]): Any = {
    val fields: Array[Field] = new Array[Field](3)
    // same column name, but name as boolean type
    fields(0) = new Field("name", DataTypes.BOOLEAN)
    fields(1) = new Field("age", DataTypes.INT)
    fields(2) = new Field("height", DataTypes.DOUBLE)

    try {
      val builder = CarbonWriter.builder()
      val writer =
        builder.outputPath(writerPath)
          .uniqueIdentifier(System.currentTimeMillis()).withBlockSize(2).sortBy(sortColumns)
          .withCsvInput(new Schema(fields)).writtenBy("TestNonTransactionalCarbonTable").build()
      var i = 0
      while (i < rows) {
        writer.write(Array[String]("true", String.valueOf(i), String.valueOf(i.toDouble / 2)))
        i += 1
      }
      writer.close()
    } catch {
      case ex: Throwable => throw new RuntimeException(ex)
    }
  }

  // prepare sdk writer output
  def buildTestDataWithSameUUID(rows: Int,
                                options: util.Map[String, String],
                                sortColumns: List[String]): Any = {
    val schema = new StringBuilder()
      .append("[ \n")
      .append("   {\"name\":\"string\"},\n")
      .append("   {\"age\":\"int\"},\n")
      .append("   {\"height\":\"double\"}\n")
      .append("]")
      .toString()

    try {
      val builder = CarbonWriter.builder()
      val writer =
        builder.outputPath(writerPath)
          .sortBy(sortColumns.toArray)
          .uniqueIdentifier(
            123).withBlockSize(2)
          .withCsvInput(Schema.parseJson(schema)).writtenBy("TestNonTransactionalCarbonTable").build()
      var i = 0
      while (i < rows) {
        writer.write(Array[String]("robot" + i, String.valueOf(i), String.valueOf(i.toDouble / 2)))
        i += 1
      }
      writer.close()
    } catch {
      case ex: Throwable => throw new RuntimeException(ex)
    }
  }

  def cleanTestData() = {
    FileUtils.deleteDirectory(new File(writerPath))
  }

  def deleteFile(path: String, extension: String): Unit = {
    val file: CarbonFile = FileFactory
      .getCarbonFile(path, FileFactory.getFileType(path))

    for (eachDir <- file.listFiles) {
      if (!eachDir.isDirectory) {
        if (eachDir.getName.endsWith(extension)) {
          CarbonUtil.deleteFoldersAndFilesSilent(eachDir)
        }
      } else {
        deleteFile(eachDir.getPath, extension)
      }
    }
  }

  override def beforeAll(): Unit = {
    CarbonProperties.getInstance()
      .addProperty(CarbonCommonConstants.CARBON_TIMESTAMP_FORMAT,
        CarbonCommonConstants.CARBON_TIMESTAMP_DEFAULT_FORMAT)
    sql("DROP TABLE IF EXISTS sdkOutputTable")
  }

  override def afterAll(): Unit = {
    sql("DROP TABLE IF EXISTS sdkOutputTable")
  }

  test("test create external table with sort columns") {
    buildTestDataWithSortColumns(List("age", "name"))
    assert(new File(writerPath).exists())

    val path2 = "/Users/xubo/Desktop/xubo/git/carbondata2/examples/spark2/target/store/default"
    val df = sqlContext.read.format("carbon").load(path2)
    df.show()

    df.write
      .format("carbondata")
      .option("tableName", "testxubo")
      .mode(SaveMode.Overwrite)
      .save(writerPath + "testxubo")
    println(writerPath + "testxubo")

    //    cleanTestData()
  }
  test("test create external table with sort columns2") {
    buildTestDataWithSortColumns(List("age", "name"))
    assert(new File(writerPath).exists())

    val df = sqlContext
      .read
      .format("carbondata")
      .load("/Users/xubo/Desktop/xubo/git/carbondata1/integration/spark-common/target/warehouse/testxubo")
    df.show()

    df.write
      .format("carbondata")
      .option("tableName", "test")
      .mode(SaveMode.Overwrite)
      .save(writerPath + "test3")
    println(writerPath + "test3")

    //    cleanTestData()
  }

  test("test create external table with sort columns3") {
    buildTestDataWithSortColumns(List("age", "name"))
    assert(new File(writerPath).exists())

    val path2 = "/Users/xubo/Desktop/xubo/git/carbondata2/examples/spark2/target/store/default/source"
    val df = sqlContext
      .read
      .format("carbon")
      .load(path2)
    df.show()

    println(path2)
  }


  test("test create external table with sort columns4") {

    sql("drop table if exists testUsingCarbon")
    val path2 = "/Users/xubo/Desktop/xubo/git/carbondata2/examples/spark2/target/store/default/source/Fact/Part0/Segment_0/"
    sql(s"""create table testUsingCarbon using carbon options(path '$path2')""")
    sql("select * from testUsingCarbon").show()

    println(path2)
  }


  // --------------------------------------------- AVRO test cases ---------------------------
  def WriteFilesWithAvroWriter(rows: Int,
                               mySchema: String,
                               json: String) = {
    // conversion to GenericData.Record
    val nn = new avro.Schema.Parser().parse(mySchema)
    val record = testUtil.jsonToAvro(json, mySchema)
    try {
      val writer = CarbonWriter.builder
        .outputPath(writerPath)
        .uniqueIdentifier(System.currentTimeMillis()).withAvroInput(nn).writtenBy("TestNonTransactionalCarbonTable").build()
      var i = 0
      while (i < rows) {
        writer.write(record)
        i = i + 1
      }
      writer.close()
    }
    catch {
      case e: Exception => {
        e.printStackTrace()
        Assert.fail(e.getMessage)
      }
    }
  }

  // struct type test
  def buildAvroTestDataStruct(rows: Int, options: util.Map[String, String]): Any = {
    FileUtils.deleteDirectory(new File(writerPath))
    val mySchema =
      """
        |{"name": "address",
        | "type": "record",
        | "fields": [
        |  { "name": "name", "type": "string"},
        |  { "name": "age", "type": "float"},
        |  { "name": "address",  "type": {
        |    "type" : "record",  "name" : "my_address",
        |        "fields" : [
        |    {"name": "street", "type": "string"},
        |    {"name": "city", "type": "string"}]}}
        |]}
      """.stripMargin

    val json = """ {"name":"bob", "age":10.24, "address" : {"street":"abc", "city":"bang"}} """

    WriteFilesWithAvroWriter(rows, mySchema, json)
  }

  def buildAvroTestDataStructType(): Any = {
    FileUtils.deleteDirectory(new File(writerPath))
    buildAvroTestDataStruct(3, null)
  }

  // array type test
  def buildAvroTestDataArrayType(rows: Int, options: util.Map[String, String]): Any = {
    FileUtils.deleteDirectory(new File(writerPath))

    val mySchema =
      """ {
        |      "name": "address",
        |      "type": "record",
        |      "fields": [
        |      {
        |      "name": "name",
        |      "type": "string"
        |      },
        |      {
        |      "name": "age",
        |      "type": "int"
        |      },
        |      {
        |      "name": "address",
        |      "type": {
        |      "type": "array",
        |      "items": {
        |      "name": "street",
        |      "type": "string"
        |      }
        |      }
        |      }
        |      ]
        |  }
      """.stripMargin

    val json: String = """ {"name": "bob","age": 10,"address": ["abc", "defg"]} """

    WriteFilesWithAvroWriter(rows, mySchema, json)
  }

  def buildAvroTestDataSingleFileArrayType(): Any = {
    FileUtils.deleteDirectory(new File(writerPath))
    buildAvroTestDataArrayType(3, null)
  }

  // struct with array type test
  def buildAvroTestDataStructWithArrayType(rows: Int, options: util.Map[String, String]): Any = {
    FileUtils.deleteDirectory(new File(writerPath))

    val mySchema =
      """
                     {
                     |     "name": "address",
                     |     "type": "record",
                     |     "fields": [
                     |     { "name": "name", "type": "string"},
                     |     { "name": "age", "type": "int"},
                     |     {
                     |     "name": "address",
                     |     "type": {
                     |     "type" : "record",
                     |     "name" : "my_address",
                     |     "fields" : [
                     |     {"name": "street", "type": "string"},
                     |     {"name": "city", "type": "string"}
                     |     ]}
                     |     },
                     |     {"name" :"doorNum",
                     |     "type" : {
                     |     "type" :"array",
                     |     "items":{
                     |     "name" :"EachdoorNums",
                     |     "type" : "int",
                     |     "default":-1
                     |     }}
                     |     }]}
                     """.stripMargin

    val json =
      """ {"name":"bob", "age":10,
        |"address" : {"street":"abc", "city":"bang"},
        |"doorNum" : [1,2,3,4]}""".stripMargin

    WriteFilesWithAvroWriter(rows, mySchema, json)
  }

  def buildAvroTestDataBothStructArrayType(): Any = {
    FileUtils.deleteDirectory(new File(writerPath))
    buildAvroTestDataStructWithArrayType(3, null)
  }


  // ArrayOfStruct test
  def buildAvroTestDataArrayOfStruct(rows: Int, options: util.Map[String, String]): Any = {
    FileUtils.deleteDirectory(new File(writerPath))

    val mySchema =
      """ {
        |	"name": "address",
        |	"type": "record",
        |	"fields": [
        |		{
        |			"name": "name",
        |			"type": "string"
        |		},
        |		{
        |			"name": "age",
        |			"type": "int"
        |		},
        |		{
        |			"name": "doorNum",
        |			"type": {
        |				"type": "array",
        |				"items": {
        |					"type": "record",
        |					"name": "my_address",
        |					"fields": [
        |						{
        |							"name": "street",
        |							"type": "string"
        |						},
        |						{
        |							"name": "city",
        |							"type": "string"
        |						}
        |					]
        |				}
        |			}
        |		}
        |	]
        |} """.stripMargin
    val json =
      """ {"name":"bob","age":10,"doorNum" :
        |[{"street":"abc","city":"city1"},
        |{"street":"def","city":"city2"},
        |{"street":"ghi","city":"city3"},
        |{"street":"jkl","city":"city4"}]} """.stripMargin

    WriteFilesWithAvroWriter(rows, mySchema, json)
  }

  def buildAvroTestDataArrayOfStructType(): Any = {
    FileUtils.deleteDirectory(new File(writerPath))
    buildAvroTestDataArrayOfStruct(3, null)
  }


  // StructOfArray test
  def buildAvroTestDataStructOfArray(rows: Int, options: util.Map[String, String]): Any = {
    FileUtils.deleteDirectory(new File(writerPath))

    val mySchema =
      """ {
        |	"name": "address",
        |	"type": "record",
        |	"fields": [
        |		{
        |			"name": "name",
        |			"type": "string"
        |		},
        |		{
        |			"name": "age",
        |			"type": "int"
        |		},
        |		{
        |			"name": "address",
        |			"type": {
        |				"type": "record",
        |				"name": "my_address",
        |				"fields": [
        |					{
        |						"name": "street",
        |						"type": "string"
        |					},
        |					{
        |						"name": "city",
        |						"type": "string"
        |					},
        |					{
        |						"name": "doorNum",
        |						"type": {
        |							"type": "array",
        |							"items": {
        |								"name": "EachdoorNums",
        |								"type": "int",
        |								"default": -1
        |							}
        |						}
        |					}
        |				]
        |			}
        |		}
        |	]
        |} """.stripMargin

    val json =
      """ {
        |	"name": "bob",
        |	"age": 10,
        |	"address": {
        |		"street": "abc",
        |		"city": "bang",
        |		"doorNum": [
        |			1,
        |			2,
        |			3,
        |			4
        |		]
        |	}
        |} """.stripMargin

    WriteFilesWithAvroWriter(rows, mySchema, json)
  }

  def buildAvroTestDataStructOfArrayType(): Any = {
    FileUtils.deleteDirectory(new File(writerPath))
    buildAvroTestDataStructOfArray(3, null)
  }

  // ArrayOfStruct test
  def buildAvroTestDataArrayOfStructWithNoSortCol(rows: Int, options: util.Map[String, String]): Any = {
    FileUtils.deleteDirectory(new File(writerPath))

    val mySchema =
      """ {
        |	"name": "address",
        |	"type": "record",
        |	"fields": [
        |		{
        |			"name": "exp",
        |			"type": "int"
        |		},
        |		{
        |			"name": "age",
        |			"type": "int"
        |		},
        |		{
        |			"name": "doorNum",
        |			"type": {
        |				"type": "array",
        |				"items": {
        |					"type": "record",
        |					"name": "my_address",
        |					"fields": [
        |						{
        |							"name": "street",
        |							"type": "string"
        |						},
        |						{
        |							"name": "city",
        |							"type": "string"
        |						}
        |					]
        |				}
        |			}
        |		}
        |	]
        |} """.stripMargin
    val json =
      """ {"exp":5,"age":10,"doorNum" :
        |[{"street":"abc","city":"city1"},
        |{"street":"def","city":"city2"},
        |{"street":"ghi","city":"city3"},
        |{"street":"jkl","city":"city4"}]} """.stripMargin

    WriteFilesWithAvroWriter(rows, mySchema, json)
  }

  test("Read sdk writer Avro output Record Type with no sort columns") {
    buildAvroTestDataArrayOfStructWithNoSortCol(3, null)
    assert(new File(writerPath).exists())
    sql("DROP TABLE IF EXISTS sdkOutputTable")
    sql(
      s"""CREATE EXTERNAL TABLE sdkOutputTable STORED BY 'carbondata' LOCATION
         |'$writerPath' """.stripMargin)

    sql("desc formatted sdkOutputTable").show(false)
    sql("select * from sdkOutputTable").show(false)

    /*
    +---+---+----------------------------------------------------+
    |exp|age|doorNum                                             |
    +---+---+----------------------------------------------------+
    |5  |10 |[[abc,city1], [def,city2], [ghi,city3], [jkl,city4]]|
    |5  |10 |[[abc,city1], [def,city2], [ghi,city3], [jkl,city4]]|
    |5  |10 |[[abc,city1], [def,city2], [ghi,city3], [jkl,city4]]|
    +---+---+----------------------------------------------------+
    */
    sql("DROP TABLE sdkOutputTable")
    // drop table should not delete the files
    assert(new File(writerPath).listFiles().length > 0)
  }

  test("Read sdk writer Avro output Record Type") {
    buildAvroTestDataStructType()
    assert(new File(writerPath).exists())
    sql("DROP TABLE IF EXISTS sdkOutputTable")
    sql(
      s"""CREATE EXTERNAL TABLE sdkOutputTable STORED BY 'carbondata' LOCATION
         |'$writerPath' """.stripMargin)

    checkAnswer(sql("select * from sdkOutputTable"), Seq(
      Row("bob", 10.24f, Row("abc", "bang")),
      Row("bob", 10.24f, Row("abc", "bang")),
      Row("bob", 10.24f, Row("abc", "bang"))))

    sql("DROP TABLE sdkOutputTable")
    // drop table should not delete the files
    assert(new File(writerPath).listFiles().length > 0)
  }

  test("Read sdk writer Avro output Array Type") {
    buildAvroTestDataSingleFileArrayType()
    assert(new File(writerPath).exists())
    sql("DROP TABLE IF EXISTS sdkOutputTable")
    sql(
      s"""CREATE EXTERNAL TABLE sdkOutputTable STORED BY 'carbondata' LOCATION
         |'$writerPath' """.stripMargin)

    sql("select * from sdkOutputTable").show(200, false)

    checkAnswer(sql("select * from sdkOutputTable"), Seq(
      Row("bob", 10, new mutable.WrappedArray.ofRef[String](Array("abc", "defg"))),
      Row("bob", 10, new mutable.WrappedArray.ofRef[String](Array("abc", "defg"))),
      Row("bob", 10, new mutable.WrappedArray.ofRef[String](Array("abc", "defg")))))

    sql("DROP TABLE sdkOutputTable")
    // drop table should not delete the files
    assert(new File(writerPath).listFiles().length > 0)
    cleanTestData()
  }

  // array type Default value test
  def buildAvroTestDataArrayDefaultType(rows: Int, options: util.Map[String, String]): Any = {
    FileUtils.deleteDirectory(new File(writerPath))

    val mySchema =
      """ {
        |      "name": "address",
        |      "type": "record",
        |      "fields": [
        |      {
        |      "name": "name",
        |      "type": "string"
        |      },
        |      {
        |      "name": "age",
        |      "type": "int"
        |      },
        |      {
        |      "name": "address",
        |      "type": {
        |      "type": "array",
        |      "items": "string"
        |      },
        |      "default": ["sc","ab"]
        |      }
        |      ]
        |  }
      """.stripMargin

    // skip giving array value to take default values
    val json: String = "{\"name\": \"bob\",\"age\": 10}"

    WriteFilesWithAvroWriter(rows, mySchema, json)
  }

  def buildAvroTestDataSingleFileArrayDefaultType(): Any = {
    FileUtils.deleteDirectory(new File(writerPath))
    buildAvroTestDataArrayDefaultType(3, null)
  }

  test("Read sdk writer Avro output Array Type with Default value") {
    // avro1.8.x Parser donot handles default value , this willbe fixed in 1.9.x. So for now this
    // will throw exception. After upgradation of Avro we can change this test case.
    val exception = intercept[RuntimeException] {
      buildAvroTestDataSingleFileArrayDefaultType()
    }
    assert(exception.getMessage.contains("Expected array-start. Got END_OBJECT"))
    /*assert(new File(writerPath).exists())
    sql("DROP TABLE IF EXISTS sdkOutputTable")
    sql(
      s"""CREATE EXTERNAL TABLE sdkOutputTable STORED BY 'carbondata' LOCATION
         |'$writerPath' """.stripMargin)

    sql("select * from sdkOutputTable").show(200,false)

    checkAnswer(sql("select * from sdkOutputTable"), Seq(
      Row("bob", 10, new mutable.WrappedArray.ofRef[String](Array("sc", "ab"))),
      Row("bob", 10, new mutable.WrappedArray.ofRef[String](Array("sc", "ab"))),
      Row("bob", 10, new mutable.WrappedArray.ofRef[String](Array("sc", "ab")))))

    sql("DROP TABLE sdkOutputTable")
    // drop table should not delete the files
    assert(new File(writerPath).listFiles().length > 0)
    cleanTestData()*/
  }


  test("Read sdk writer Avro output with both Array and Struct Type") {
    buildAvroTestDataBothStructArrayType()
    assert(new File(writerPath).exists())
    sql("DROP TABLE IF EXISTS sdkOutputTable")
    sql(
      s"""CREATE EXTERNAL TABLE sdkOutputTable STORED BY 'carbondata' LOCATION
         |'$writerPath' """.stripMargin)

    /*
    *-+----+---+----------+------------+
    |name|age|address   |doorNum     |
    +----+---+----------+------------+
    |bob |10 |[abc,bang]|[1, 2, 3, 4]|
    |bob |10 |[abc,bang]|[1, 2, 3, 4]|
    |bob |10 |[abc,bang]|[1, 2, 3, 4]|
    +----+---+----------+------------+
    * */

    checkAnswer(sql("select * from sdkOutputTable"), Seq(
      Row("bob", 10, Row("abc", "bang"), mutable.WrappedArray.newBuilder[Int].+=(1, 2, 3, 4)),
      Row("bob", 10, Row("abc", "bang"), mutable.WrappedArray.newBuilder[Int].+=(1, 2, 3, 4)),
      Row("bob", 10, Row("abc", "bang"), mutable.WrappedArray.newBuilder[Int].+=(1, 2, 3, 4))))
    sql("DROP TABLE sdkOutputTable")
    // drop table should not delete the files
    cleanTestData()
  }


  test("Read sdk writer Avro output with Array of struct") {
    buildAvroTestDataArrayOfStructType()
    assert(new File(writerPath).exists())
    sql("DROP TABLE IF EXISTS sdkOutputTable")
    sql(
      s"""CREATE EXTERNAL TABLE sdkOutputTable STORED BY 'carbondata' LOCATION
         |'$writerPath' """.stripMargin)

    sql("select * from sdkOutputTable").show(false)

    // TODO: Add a validation
    /*
    +----+---+----------------------------------------------------+
    |name|age|doorNum                                             |
    +----+---+----------------------------------------------------+
    |bob |10 |[[abc,city1], [def,city2], [ghi,city3], [jkl,city4]]|
    |bob |10 |[[abc,city1], [def,city2], [ghi,city3], [jkl,city4]]|
    |bob |10 |[[abc,city1], [def,city2], [ghi,city3], [jkl,city4]]|
    +----+---+----------------------------------------------------+ */

    sql("DROP TABLE sdkOutputTable")
    // drop table should not delete the files
    cleanTestData()
  }


  // Struct of array
  test("Read sdk writer Avro output with struct of Array") {
    buildAvroTestDataStructOfArrayType()
    assert(new File(writerPath).exists())
    sql("DROP TABLE IF EXISTS sdkOutputTable")
    sql(
      s"""CREATE EXTERNAL TABLE sdkOutputTable STORED BY 'carbondata' LOCATION
         |'$writerPath' """.stripMargin)

    sql("SELECT name,name FROM sdkOutputTable").show()
    checkAnswer(sql("SELECT name,name FROM sdkOutputTable"), Seq(
      Row("bob", "bob"),
      Row("bob", "bob"),
      Row("bob", "bob")))

    sql("select * from sdkOutputTable").show(false)

    // TODO: Add a validation
    /*
    +----+---+-------------------------------------------------------+
    |name|age|address                                                |
    +----+---+-------------------------------------------------------+
    |bob |10 |[abc,bang,WrappedArray(1, 2, 3, 4)]                    |
    |bob |10 |[abc,bang,WrappedArray(1, 2, 3, 4)]                    |
    |bob |10 |[abc,bang,WrappedArray(1, 2, 3, 4)]                    |
    +----+---+-------------------------------------------------------+*/

    sql("DROP TABLE sdkOutputTable")
    // drop table should not delete the files
    cleanTestData()
  }

  // test multi level -- 3 levels [array of struct of array of int]
  def buildAvroTestDataMultiLevel3(rows: Int, options: util.Map[String, String]): Any = {
    FileUtils.deleteDirectory(new File(writerPath))

    val mySchema =
      """ {
        |	"name": "address",
        |	"type": "record",
        |	"fields": [
        |		{
        |			"name": "name",
        |			"type": "string"
        |		},
        |		{
        |			"name": "age",
        |			"type": "int"
        |		},
        |		{
        |			"name": "doorNum",
        |			"type": {
        |				"type": "array",
        |				"items": {
        |					"type": "record",
        |					"name": "my_address",
        |					"fields": [
        |						{
        |							"name": "street",
        |							"type": "string"
        |						},
        |						{
        |							"name": "city",
        |							"type": "string"
        |						},
        |						{
        |							"name": "FloorNum",
        |							"type": {
        |								"type": "array",
        |								"items": {
        |									"name": "floor",
        |									"type": "int"
        |								}
        |							}
        |						}
        |					]
        |				}
        |			}
        |		}
        |	]
        |} """.stripMargin
    val json =
      """ {
        |	"name": "bob",
        |	"age": 10,
        |	"doorNum": [
        |		{
        |			"street": "abc",
        |			"city": "city1",
        |			"FloorNum": [0,1,2]
        |		},
        |		{
        |			"street": "def",
        |			"city": "city2",
        |			"FloorNum": [3,4,5]
        |		},
        |		{
        |			"street": "ghi",
        |			"city": "city3",
        |			"FloorNum": [6,7,8]
        |		},
        |		{
        |			"street": "jkl",
        |			"city": "city4",
        |			"FloorNum": [9,10,11]
        |		}
        |	]
        |} """.stripMargin

    WriteFilesWithAvroWriter(rows, mySchema, json)
  }

  def buildAvroTestDataMultiLevel3Type(): Any = {
    FileUtils.deleteDirectory(new File(writerPath))
    buildAvroTestDataMultiLevel3(3, null)
  }

  // test multi level -- 3 levels [array of struct of array of int]
  test("test multi level support : array of struct of array of int") {
    buildAvroTestDataMultiLevel3Type()
    assert(new File(writerPath).exists())
    sql("DROP TABLE IF EXISTS sdkOutputTable")
    sql(
      s"""CREATE EXTERNAL TABLE sdkOutputTable STORED BY 'carbondata' LOCATION
         |'$writerPath' """.stripMargin)

    sql("select * from sdkOutputTable").show(false)

    // TODO: Add a validation
    /*
    +----+---+-----------------------------------------------------------------------------------+
    |name|age|doorNum
                                                               |
    +----+---+-----------------------------------------------------------------------------------+
    |bob |10 |[[abc,city1,WrappedArray(0, 1, 2)], [def,city2,WrappedArray(3, 4, 5)], [ghi,city3,
    WrappedArray(6, 7, 8)], [jkl,city4,WrappedArray(9, 10, 11)]]|
    |bob |10 |[[abc,city1,WrappedArray(0, 1, 2)], [def,city2,WrappedArray(3, 4, 5)], [ghi,city3,
    WrappedArray(6, 7, 8)], [jkl,city4,WrappedArray(9, 10, 11)]]|
    |bob |10 |[[abc,city1,WrappedArray(0, 1, 2)], [def,city2,WrappedArray(3, 4, 5)], [ghi,city3,
    WrappedArray(6, 7, 8)], [jkl,city4,WrappedArray(9, 10, 11)]]|
    +----+---+-----------------------------------------------------------------------------------+*/

    sql("DROP TABLE sdkOutputTable")
    // drop table should not delete the files
    cleanTestData()
  }


  // test multi level -- 3 levels [array of struct of struct of string, int]
  def buildAvroTestDataMultiLevel3_1(rows: Int, options: util.Map[String, String]): Any = {
    FileUtils.deleteDirectory(new File(writerPath))

    val mySchema =
      """ {
        |	"name": "address",
        |	"type": "record",
        |	"fields": [
        |		{
        |			"name": "name",
        |			"type": "string"
        |		},
        |		{
        |			"name": "age",
        |			"type": "int"
        |		},
        |		{
        |			"name": "doorNum",
        |			"type": {
        |				"type": "array",
        |				"items": {
        |					"type": "record",
        |					"name": "my_address",
        |					"fields": [
        |						{
        |							"name": "street",
        |							"type": "string"
        |						},
        |						{
        |							"name": "city",
        |							"type": "string"
        |						},
        |						{
        |							"name": "FloorNum",
        |                			"type": {
        |                				"type": "record",
        |                				"name": "Floor",
        |                				"fields": [
        |                					{
        |                						"name": "wing",
        |                						"type": "string"
        |                					},
        |                					{
        |                						"name": "number",
        |                						"type": "int"
        |                					}
        |                				]
        |                			}
        |						}
        |					]
        |				}
        |			}
        |		}
        |	]
        |} """.stripMargin


    val json =
      """  {
        |	"name": "bob",
        |	"age": 10,
        |	"doorNum": [
        |		{
        |			"street": "abc",
        |			"city": "city1",
        |			"FloorNum": {"wing" : "a", "number" : 1}
        |		},
        |		{
        |			"street": "def",
        |			"city": "city2",
        |			"FloorNum": {"wing" : "b", "number" : 0}
        |		},
        |		{
        |			"street": "ghi",
        |			"city": "city3",
        |			"FloorNum": {"wing" : "a", "number" : 2}
        |		}
        |	]
        |}  """.stripMargin

    WriteFilesWithAvroWriter(rows, mySchema, json)
  }

  def buildAvroTestDataMultiLevel3_1Type(): Any = {
    FileUtils.deleteDirectory(new File(writerPath))
    buildAvroTestDataMultiLevel3_1(3, null)
  }

  // test multi level -- 3 levels [array of struct of struct of string, int]
  test("test multi level support : array of struct of struct") {
    buildAvroTestDataMultiLevel3_1Type()
    assert(new File(writerPath).exists())
    sql("DROP TABLE IF EXISTS sdkOutputTable")
    sql(
      s"""CREATE EXTERNAL TABLE sdkOutputTable STORED BY 'carbondata' LOCATION
         |'$writerPath' """.stripMargin)

    sql("select * from sdkOutputTable").show(false)

    // TODO: Add a validation
    /*
    +----+---+---------------------------------------------------------+
    |name|age|doorNum                                                  |
    +----+---+---------------------------------------------------------+
    |bob |10 |[[abc,city1,[a,1]], [def,city2,[b,0]], [ghi,city3,[a,2]]]|
    |bob |10 |[[abc,city1,[a,1]], [def,city2,[b,0]], [ghi,city3,[a,2]]]|
    |bob |10 |[[abc,city1,[a,1]], [def,city2,[b,0]], [ghi,city3,[a,2]]]|
    +----+---+---------------------------------------------------------+
    */

    sql("DROP TABLE sdkOutputTable")
    // drop table should not delete the files
    cleanTestData()
  }

  // test multi level -- 3 levels [array of array of array of int]
  def buildAvroTestDataMultiLevel3_2(rows: Int, options: util.Map[String, String]): Any = {
    FileUtils.deleteDirectory(new File(writerPath))

    val mySchema =
      """ {
        |	"name": "address",
        |	"type": "record",
        |	"fields": [
        |		{
        |			"name": "name",
        |			"type": "string"
        |		},
        |		{
        |			"name": "age",
        |			"type": "int"
        |		},
        |		{
        |			"name": "BuildNum",
        |			"type": {
        |				"type": "array",
        |				"items": {
        |					"name": "FloorNum",
        |					"type": "array",
        |					"items": {
        |						"name": "doorNum",
        |						"type": "array",
        |						"items": {
        |							"name": "EachdoorNums",
        |							"type": "int",
        |              "logicalType": "date",
        |							"default": -1
        |						}
        |					}
        |				}
        |			}
        |		}
        |	]
        |} """.stripMargin

    val json =
      """   {
        |        	"name": "bob",
        |        	"age": 10,
        |        	"BuildNum": [[[1,2,3],[4,5,6]],[[10,20,30],[40,50,60]]]
        |        }   """.stripMargin

    WriteFilesWithAvroWriter(rows, mySchema, json)
  }

  def buildAvroTestDataMultiLevel3_2Type(): Any = {
    FileUtils.deleteDirectory(new File(writerPath))
    buildAvroTestDataMultiLevel3_2(3, null)
  }

  // test multi level -- 3 levels [array of array of array of int with logical type]
  test("test multi level support : array of array of array of int with logical type") {
    buildAvroTestDataMultiLevel3_2Type()
    assert(new File(writerPath).exists())
    sql("DROP TABLE IF EXISTS sdkOutputTable")
    sql(
      s"""CREATE EXTERNAL TABLE sdkOutputTable STORED BY 'carbondata' LOCATION
         |'$writerPath' """.stripMargin)

    sql("select * from sdkOutputTable limit 1").show(false)

    // TODO: Add a validation
    /*
    +----+---+------------------------------------------------------------------+
    |name|age|BuildNum                                                          |
    +----+---+------------------------------------------------------------------+
    |bob |10 |[WrappedArray(WrappedArray(1970-01-02, 1970-01-03, 1970-01-04),   |
    |                    WrappedArray(1970-01-05, 1970-01-06, 1970-01-07)),     |
    |       WrappedArray(WrappedArray(1970-01-11, 1970-01-21, 1970-01-31),      |
    |                    WrappedArray(1970-02-10, 1970-02-20, 1970-03-02))]     |
    +----+---+------------------------------------------------------------------+
     */

    sql("DROP TABLE sdkOutputTable")
    // drop table should not delete the files
    cleanTestData()
  }


  // test multi level -- 4 levels [array of array of array of struct]
  def buildAvroTestDataMultiLevel4(rows: Int, options: util.Map[String, String]): Any = {
    FileUtils.deleteDirectory(new File(writerPath))

    val mySchema =
      """ {
        |	"name": "address",
        |	"type": "record",
        |	"fields": [
        |		{
        |			"name": "name",
        |			"type": "string"
        |		},
        |		{
        |			"name": "age",
        |			"type": "int"
        |		},
        |		{
        |			"name": "BuildNum",
        |			"type": {
        |				"type": "array",
        |				"items": {
        |					"name": "FloorNum",
        |					"type": "array",
        |					"items": {
        |						"name": "doorNum",
        |						"type": "array",
        |						"items": {
        |							"name": "my_address",
        |							"type": "record",
        |							"fields": [
        |								{
        |									"name": "street",
        |									"type": "string"
        |								},
        |								{
        |									"name": "city",
        |									"type": "string"
        |								}
        |							]
        |						}
        |					}
        |				}
        |			}
        |		}
        |	]
        |} """.stripMargin

    val json =
      """ {
        |	"name": "bob",
        |	"age": 10,
        |	"BuildNum": [
        |		[
        |			[
        |				{"street":"abc", "city":"city1"},
        |				{"street":"def", "city":"city2"},
        |				{"street":"cfg", "city":"city3"}
        |			],
        |			[
        |				 {"street":"abc1", "city":"city3"},
        |				 {"street":"def1", "city":"city4"},
        |				 {"street":"cfg1", "city":"city5"}
        |			]
        |		],
        |		[
        |			[
        |				 {"street":"abc2", "city":"cityx"},
        |				 {"street":"abc3", "city":"cityy"},
        |				 {"street":"abc4", "city":"cityz"}
        |			],
        |			[
        |				 {"street":"a1bc", "city":"cityA"},
        |				 {"street":"a1bc", "city":"cityB"},
        |				 {"street":"a1bc", "city":"cityc"}
        |			]
        |		]
        |	]
        |} """.stripMargin

    WriteFilesWithAvroWriter(rows, mySchema, json)
  }

  def buildAvroTestDataMultiLevel4Type(): Any = {
    FileUtils.deleteDirectory(new File(writerPath))
    buildAvroTestDataMultiLevel4(3, null)
  }

  // test multi level -- 4 levels [array of array of array of struct]
  test("test multi level support : array of array of array of struct") {
    buildAvroTestDataMultiLevel4Type()
    assert(new File(writerPath).exists())
    sql("DROP TABLE IF EXISTS sdkOutputTable")
    sql(
      s"""CREATE EXTERNAL TABLE sdkOutputTable STORED BY 'carbondata' LOCATION
         |'$writerPath' """.stripMargin)

    sql("select * from sdkOutputTable").show(false)

    // TODO: Add a validation

    sql("DROP TABLE sdkOutputTable")
    // drop table should not delete the files
    cleanTestData()
  }

  test(
    "test if exception is thrown when a column which is not in schema is specified in sort columns") {
    val schema1 =
      """{
        |	"namespace": "com.apache.schema",
        |	"type": "record",
        |	"name": "StudentActivity",
        |	"fields": [
        |		{
        |			"name": "id",
        |			"type": "int"
        |		},
        |		{
        |			"name": "course_details",
        |			"type": {
        |				"name": "course_details",
        |				"type": "record",
        |				"fields": [
        |					{
        |						"name": "course_struct_course_time",
        |						"type": "string"
        |					}
        |				]
        |			}
        |		}
        |	]
        |}""".stripMargin

    val json1 =
      """{"id": 101,"course_details": { "course_struct_course_time":"2014-01-05"  }}""".stripMargin

    val nn = new org.apache.avro.Schema.Parser().parse(schema1)
    val record = testUtil.jsonToAvro(json1, schema1)

    assert(intercept[RuntimeException] {
      val writer = CarbonWriter.builder.sortBy(Array("name", "id"))
        .outputPath(writerPath).withAvroInput(nn).writtenBy("TestNonTransactionalCarbonTable").build()
      writer.write(record)
      writer.close()
    }.getMessage.toLowerCase.contains("column: name specified in sort columns"))
  }

  test("test if load is passing with NULL type") {
    val schema1 =
      """{
        |	"namespace": "com.apache.schema",
        |	"type": "record",
        |	"name": "StudentActivity",
        |	"fields": [
        |		{
        |			"name": "id",
        |			"type": "null"
        |		},
        |		{
        |			"name": "course_details",
        |			"type": {
        |				"name": "course_details",
        |				"type": "record",
        |				"fields": [
        |					{
        |						"name": "course_struct_course_time",
        |						"type": "string"
        |					}
        |				]
        |			}
        |		}
        |	]
        |}""".stripMargin

    val json1 =
      """{"id": null,"course_details": { "course_struct_course_time":"2014-01-05"  }}""".stripMargin

    val nn = new org.apache.avro.Schema.Parser().parse(schema1)
    val record = testUtil.jsonToAvro(json1, schema1)

    val writer = CarbonWriter.builder
      .outputPath(writerPath).withAvroInput(nn).writtenBy("TestNonTransactionalCarbonTable").build()
    writer.write(record)
    writer.close()
  }

  test("test if data load is success with a struct having timestamp column  ") {
    val schema1 =
      """{
        |	"namespace": "com.apache.schema",
        |	"type": "record",
        |	"name": "StudentActivity",
        |	"fields": [
        |		{
        |			"name": "id",
        |			"type": "int"
        |		},
        |		{
        |			"name": "course_details",
        |			"type": {
        |				"name": "course_details",
        |				"type": "record",
        |				"fields": [
        |					{
        |						"name": "course_struct_course_time",
        |						"type": "string"
        |					}
        |				]
        |			}
        |		}
        |	]
        |}""".stripMargin

    val json1 =
      """{"id": 101,"course_details": { "course_struct_course_time":"2014-01-05 00:00:00"  }}""".stripMargin
    val nn = new org.apache.avro.Schema.Parser().parse(schema1)
    val record = testUtil.jsonToAvro(json1, schema1)

    val writer = CarbonWriter.builder.sortBy(Array("id"))
      .outputPath(writerPath).withAvroInput(nn).writtenBy("TestNonTransactionalCarbonTable").build()
    writer.write(record)
    writer.close()
  }

  test(
    "test is dataload is successful if childcolumn has same name as one of the other fields(not " +
      "complex)") {
    val schema =
      """{
        |	"type": "record",
        |	"name": "Order",
        |	"namespace": "com.apache.schema",
        |	"fields": [
        |		{
        |			"name": "id",
        |			"type": "long"
        |		},
        |		{
        |			"name": "entries",
        |			"type": {
        |				"type": "array",
        |				"items": {
        |					"type": "record",
        |					"name": "Entry",
        |					"fields": [
        |						{
        |							"name": "id",
        |							"type": "long"
        |						}
        |					]
        |				}
        |			}
        |		}
        |	]
        |}""".stripMargin
    val json1 =
      """{"id": 101, "entries": [ {"id":1234}, {"id":3212}  ]}""".stripMargin

    val nn = new org.apache.avro.Schema.Parser().parse(schema)
    val record = testUtil.jsonToAvro(json1, schema)

    val writer = CarbonWriter.builder
      .outputPath(writerPath).withAvroInput(nn).writtenBy("TestNonTransactionalCarbonTable").build()
    writer.write(record)
    writer.close()
  }

  test("test logical type date") {
    sql("drop table if exists sdkOutputTable")
    FileFactory.deleteAllCarbonFilesOfDir(FileFactory.getCarbonFile(writerPath))
    val schema1 =
      """{
        |	"namespace": "com.apache.schema",
        |	"type": "record",
        |	"name": "StudentActivity",
        |	"fields": [
        |		{
        |			"name": "id",
        |						"type": {"type" : "int", "logicalType": "date"}
        |		},
        |		{
        |			"name": "course_details",
        |			"type": {
        |				"name": "course_details",
        |				"type": "record",
        |				"fields": [
        |					{
        |						"name": "course_struct_course_time",
        |						"type": {"type" : "int", "logicalType": "date"}
        |					}
        |				]
        |			}
        |		}
        |	]
        |}""".stripMargin

    val json1 =
      """{"id": 101, "course_details": { "course_struct_course_time":10}}""".stripMargin
    val nn = new org.apache.avro.Schema.Parser().parse(schema1)
    val record = testUtil.jsonToAvro(json1, schema1)

    val writer = CarbonWriter.builder
      .outputPath(writerPath).withAvroInput(nn).writtenBy("TestNonTransactionalCarbonTable").build()
    writer.write(record)
    writer.close()
    sql(
      s"""CREATE EXTERNAL TABLE sdkOutputTable STORED BY
         |'carbondata' LOCATION
         |'$writerPath' """.stripMargin)
    checkAnswer(sql("select * from sdkOutputTable"), Seq(Row(java.sql.Date.valueOf("1970-04-12"), Row(java.sql.Date.valueOf("1970-01-11")))))
  }

  test("test logical type timestamp-millis") {
    sql("drop table if exists sdkOutputTable")
    FileFactory.deleteAllCarbonFilesOfDir(FileFactory.getCarbonFile(writerPath))
    val schema1 =
      """{
        |	"namespace": "com.apache.schema",
        |	"type": "record",
        |	"name": "StudentActivity",
        |	"fields": [
        |		{
        |			"name": "id",
        |						"type": {"type" : "long", "logicalType": "timestamp-millis"}
        |		},
        |		{
        |			"name": "course_details",
        |			"type": {
        |				"name": "course_details",
        |				"type": "record",
        |				"fields": [
        |					{
        |						"name": "course_struct_course_time",
        |						"type": {"type" : "long", "logicalType": "timestamp-millis"}
        |					}
        |				]
        |			}
        |		}
        |	]
        |}""".stripMargin

    val json1 =
      """{"id": 172800000,"course_details": { "course_struct_course_time":172800000}}""".stripMargin

    val nn = new org.apache.avro.Schema.Parser().parse(schema1)
    val record = testUtil.jsonToAvro(json1, schema1)

    val writer = CarbonWriter.builder
      .outputPath(writerPath).withAvroInput(nn).writtenBy("TestNonTransactionalCarbonTable").build()
    writer.write(record)
    writer.close()
    sql(
      s"""CREATE EXTERNAL TABLE sdkOutputTable STORED BY
         |'carbondata' LOCATION
         |'$writerPath' """.stripMargin)
    checkAnswer(sql("select * from sdkOutputTable"), Seq(Row(Timestamp.valueOf("1970-01-02 16:00:00"), Row(Timestamp.valueOf("1970-01-02 16:00:00")))))
  }

  test("test logical type-micros timestamp") {
    sql("drop table if exists sdkOutputTable")
    FileFactory.deleteAllCarbonFilesOfDir(FileFactory.getCarbonFile(writerPath))
    val schema1 =
      """{
        |	"namespace": "com.apache.schema",
        |	"type": "record",
        |	"name": "StudentActivity",
        |	"fields": [
        |		{
        |			"name": "id",
        |						"type": {"type" : "long", "logicalType": "timestamp-micros"}
        |		},
        |		{
        |			"name": "course_details",
        |			"type": {
        |				"name": "course_details",
        |				"type": "record",
        |				"fields": [
        |					{
        |						"name": "course_struct_course_time",
        |						"type": {"type" : "long", "logicalType": "timestamp-micros"}
        |					}
        |				]
        |			}
        |		}
        |	]
        |}""".stripMargin

    val json1 =
      """{"id": 172800000000,"course_details": { "course_struct_course_time":172800000000}}""".stripMargin

    val nn = new org.apache.avro.Schema.Parser().parse(schema1)
    val record = testUtil.jsonToAvro(json1, schema1)


    val writer = CarbonWriter.builder
      .outputPath(writerPath).withAvroInput(nn).writtenBy("TestNonTransactionalCarbonTable").build()
    writer.write(record)
    writer.close()
    sql(
      s"""CREATE EXTERNAL TABLE sdkOutputTable STORED BY
         |'carbondata' LOCATION
         |'$writerPath' """.stripMargin)
    checkAnswer(sql("select * from sdkOutputTable"), Seq(Row(Timestamp.valueOf("1970-01-02 16:00:00"), Row(Timestamp.valueOf("1970-01-02 16:00:00")))))
  }

  test("test Sort Scope with SDK") {
    cleanTestData()
    // test with no_sort
    val options = Map("sort_scope" -> "no_sort").asJava
    val fields: Array[Field] = new Array[Field](4)
    fields(0) = new Field("stringField", DataTypes.STRING)
    fields(1) = new Field("intField", DataTypes.INT)
    val writer: CarbonWriter = CarbonWriter.builder
      .outputPath(writerPath)
      .withTableProperties(options)
      .withCsvInput(new Schema(fields)).writtenBy("TestNonTransactionalCarbonTable").build()
    writer.write(Array("carbon", "1"))
    writer.write(Array("hydrogen", "10"))
    writer.write(Array("boron", "4"))
    writer.write(Array("zirconium", "5"))
    writer.close()

    // read no sort data
    sql("DROP TABLE IF EXISTS sdkTable")
    sql(
      s"""CREATE EXTERNAL TABLE sdkTable STORED BY 'carbondata' LOCATION '$writerPath' """
        .stripMargin)
    checkAnswer(sql("select * from sdkTable"),
      Seq(Row("carbon", 1), Row("hydrogen", 10), Row("boron", 4), Row("zirconium", 5)))

    // write local sort data
    val writer1: CarbonWriter = CarbonWriter.builder
      .outputPath(writerPath)
      .withCsvInput(new Schema(fields)).writtenBy("TestNonTransactionalCarbonTable").build()
    writer1.write(Array("carbon", "1"))
    writer1.write(Array("hydrogen", "10"))
    writer1.write(Array("boron", "4"))
    writer1.write(Array("zirconium", "5"))
    writer1.close()
    // read both-no sort and local sort data
    checkAnswer(sql("select count(*) from sdkTable"), Seq(Row(8)))
    sql("DROP TABLE sdkTable")
    cleanTestData()
  }

  test("test LocalDictionary with True") {
    FileUtils.deleteDirectory(new File(writerPath))
    val builder = CarbonWriter.builder
      .sortBy(Array[String]("name")).withBlockSize(12).enableLocalDictionary(true)
      .uniqueIdentifier(System.currentTimeMillis).taskNo(System.nanoTime).outputPath(writerPath).writtenBy("TestNonTransactionalCarbonTable")
    generateCarbonData(builder)
    assert(FileFactory.getCarbonFile(writerPath).exists())
    assert(testUtil.checkForLocalDictionary(testUtil.getDimRawChunk(0, writerPath)))
    sql("DROP TABLE IF EXISTS sdkTable")
    sql(
      s"""CREATE EXTERNAL TABLE sdkTable STORED BY 'carbondata' LOCATION
         |'$writerPath' """.stripMargin)
    val descLoc = sql("describe formatted sdkTable").collect
    descLoc.find(_.get(0).toString.contains("Local Dictionary Enabled")) match {
      case Some(row) => assert(row.get(1).toString.contains("true"))
      case None => assert(false)
    }
    FileUtils.deleteDirectory(new File(writerPath))
  }

  test("test LocalDictionary with custom Threshold") {
    FileUtils.deleteDirectory(new File(writerPath))
    val tablePropertiesMap: util.Map[String, String] =
      Map("table_blocksize" -> "12",
        "sort_columns" -> "name",
        "local_dictionary_threshold" -> "200",
        "local_dictionary_enable" -> "true",
        "inverted_index" -> "name").asJava
    val builder = CarbonWriter.builder
      .withTableProperties(tablePropertiesMap)
      .uniqueIdentifier(System.currentTimeMillis).taskNo(System.nanoTime).outputPath(writerPath).writtenBy("TestNonTransactionalCarbonTable")
    generateCarbonData(builder)
    assert(FileFactory.getCarbonFile(writerPath).exists())
    assert(testUtil.checkForLocalDictionary(testUtil.getDimRawChunk(0, writerPath)))
    sql("DROP TABLE IF EXISTS sdkTable")
    sql(
      s"""CREATE EXTERNAL TABLE sdkTable STORED BY 'carbondata' LOCATION
         |'$writerPath' """.stripMargin)
    val df = sql("describe formatted sdkTable")
    checkExistence(df, true, "Local Dictionary Enabled true")
    checkExistence(df, true, "Inverted Index Columns name")
    FileUtils.deleteDirectory(new File(writerPath))
  }

  test("test Local Dictionary with FallBack") {
    FileUtils.deleteDirectory(new File(writerPath))
    val builder = CarbonWriter.builder
      .sortBy(Array[String]("name")).withBlockSize(12).enableLocalDictionary(true)
      .localDictionaryThreshold(5)
      .uniqueIdentifier(System.currentTimeMillis).taskNo(System.nanoTime).outputPath(writerPath).writtenBy("TestNonTransactionalCarbonTable")
    generateCarbonData(builder)
    assert(FileFactory.getCarbonFile(writerPath).exists())
    assert(!testUtil.checkForLocalDictionary(testUtil.getDimRawChunk(0, writerPath)))
    sql("DROP TABLE IF EXISTS sdkTable")
    sql(
      s"""CREATE EXTERNAL TABLE sdkTable STORED BY 'carbondata' LOCATION
         |'$writerPath' """.stripMargin)
    val df = sql("describe formatted sdkTable")
    checkExistence(df, true, "Local Dictionary Enabled true")
    FileUtils.deleteDirectory(new File(writerPath))
  }

  test("test local dictionary with External Table data load ") {
    FileUtils.deleteDirectory(new File(writerPath))
    val builder = CarbonWriter.builder
      .sortBy(Array[String]("name")).withBlockSize(12).enableLocalDictionary(true)
      .localDictionaryThreshold(200)
      .uniqueIdentifier(System.currentTimeMillis).taskNo(System.nanoTime).outputPath(writerPath).writtenBy("TestNonTransactionalCarbonTable")
    generateCarbonData(builder)
    assert(FileFactory.getCarbonFile(writerPath).exists())
    assert(testUtil.checkForLocalDictionary(testUtil.getDimRawChunk(0, writerPath)))
    sql("DROP TABLE IF EXISTS sdkTable")
    sql(
      s"""CREATE EXTERNAL TABLE sdkTable STORED BY 'carbondata' LOCATION
         |'$writerPath' """.stripMargin)
    FileUtils.deleteDirectory(new File(writerPath))
    sql("insert into sdkTable select 's1','s2',23 ")
    assert(FileFactory.getCarbonFile(writerPath).exists())
    assert(testUtil.checkForLocalDictionary(testUtil.getDimRawChunk(0, writerPath)))
    val df = sql("describe formatted sdkTable")
    checkExistence(df, true, "Local Dictionary Enabled true")
    checkAnswer(sql("select count(*) from sdkTable"), Seq(Row(1)))
    FileUtils.deleteDirectory(new File(writerPath))
  }

  test("test inverted index column by API") {
    FileUtils.deleteDirectory(new File(writerPath))
    val builder = CarbonWriter.builder
      .sortBy(Array[String]("name")).withBlockSize(12).enableLocalDictionary(true)
      .uniqueIdentifier(System.currentTimeMillis).taskNo(System.nanoTime).outputPath(writerPath)
      .invertedIndexFor(Array[String]("name")).writtenBy("TestNonTransactionalCarbonTable")
    generateCarbonData(builder)
    sql("DROP TABLE IF EXISTS sdkTable")
    sql(
      s"""CREATE EXTERNAL TABLE sdkTable STORED BY 'carbondata' LOCATION
         |'$writerPath' """.stripMargin)
    FileUtils.deleteDirectory(new File(writerPath))
    sql("insert into sdkTable select 's1','s2',23 ")
    val df = sql("describe formatted sdkTable")
    checkExistence(df, true, "Inverted Index Columns name")
    checkAnswer(sql("select count(*) from sdkTable"), Seq(Row(1)))
    FileUtils.deleteDirectory(new File(writerPath))
  }

  def generateCarbonData(builder: CarbonWriterBuilder): Unit = {
    val fields = new Array[Field](3)
    fields(0) = new Field("name", DataTypes.STRING)
    fields(1) = new Field("surname", DataTypes.STRING)
    fields(2) = new Field("age", DataTypes.INT)
    val carbonWriter = builder.withCsvInput(new Schema(fields)).build()
    var i = 0
    while (i < 100) {
      {
        carbonWriter
          .write(Array[String]("robot" + (i % 10), "robot_surname" + (i % 10), String.valueOf(i)))
      }
      {
        i += 1; i - 1
      }
    }
    carbonWriter.close()
  }
}
