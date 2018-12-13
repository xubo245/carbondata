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

package org.apache.carbondata.examples

import java.io.File

import org.apache.hadoop.fs.s3a.Constants.{ACCESS_KEY, ENDPOINT, SECRET_KEY}
import org.apache.carbondata.core.constants.CarbonCommonConstants
import org.apache.carbondata.core.util.CarbonProperties
import org.apache.carbondata.examples.S3UsingSDKExample.getS3EndPoint
import org.apache.commons.io.FileUtils
import org.apache.hadoop.fs.s3a.Constants
import org.apache.spark.sql.types._
import org.apache.spark.sql.{DataFrame, Row, SparkSession}

/**
  * This example doesn't create carbonsession, but use CarbonSource when creating table
  */

object SparkDataFrameS3Example {

  def main(args: Array[String]): Unit = {
    val rootPath = new File(this.getClass.getResource("/").getPath
      + "../../../..").getCanonicalPath
    val storeLocation = s"$rootPath/examples/spark2/target/store"
    val warehouse = s"$rootPath/examples/spark2/target/warehouse"
    val metastoredb = s"$rootPath/examples/spark2/target/metastore_db"

    // clean data folder
    if (true) {
      val clean = (path: String) => FileUtils.deleteDirectory(new File(path))
      clean(storeLocation)
      clean(warehouse)
      clean(metastoredb)
    }

    val sparksession = SparkSession
      .builder()
      .master("local")
      .appName("SparkSessionExample")
      .enableHiveSupport()
      .config("spark.sql.warehouse.dir", warehouse)
      .getOrCreate()

    CarbonProperties.getInstance()
      .addProperty(CarbonCommonConstants.CARBON_TIMESTAMP_FORMAT, "yyyy/MM/dd HH:mm:ss")
      .addProperty(CarbonCommonConstants.CARBON_DATE_FORMAT, "yyyy/MM/dd")
      .addProperty("carbon.storelocation", storeLocation)

    sparksession.sparkContext.setLogLevel("ERROR")

    val df = sparksession.emptyDataFrame

//    read(df, path = "s3a://xubo/sdk/test", args);
    write(df, path = "s3a://xubo/sdk/testWrite2", args);

    CarbonProperties.getInstance().addProperty(
      CarbonCommonConstants.CARBON_TIMESTAMP_FORMAT,
      CarbonCommonConstants.CARBON_TIMESTAMP_DEFAULT_FORMAT)
    CarbonProperties.getInstance().addProperty(
      CarbonCommonConstants.CARBON_DATE_FORMAT,
      CarbonCommonConstants.CARBON_DATE_DEFAULT_FORMAT)

    // Drop table
    sparksession.sql("DROP TABLE IF EXISTS sparksession_table")
    sparksession.sql("DROP TABLE IF EXISTS csv_table")

    sparksession.stop()
  }

  def read(df: DataFrame, path: String, args: Array[String]): Unit = {
    import org.apache.spark.sql.CarbonSession._

    val rootPath = new File(this.getClass.getResource("/").getPath
      + "../../../..").getCanonicalPath
    val storeLocation = s"$rootPath/examples/spark2/target/store2"
    val warehouse = s"$rootPath/examples/spark2/target/warehouse2"
    val metastoredb = s"$rootPath/examples/spark2/target/metastore_db2"

    // 1. method 1, todo
    //    val carbonSession = new CarbonSession(df.sparkSession.sparkContext)

    // 2. method 2
    val carbonSession = SparkSession
      .builder()
      .master("local")
      .appName("carbon")
      .config(df.sparkSession.sparkContext.getConf)
      .config("spark.sql.crossJoin.enabled", "true")
      .config(Constants.ACCESS_KEY, args(0))
      .config(Constants.SECRET_KEY, args(1))
      .config(Constants.ENDPOINT, args(2))
      .getOrCreateCarbonSession(storeLocation, metastoredb)

    val result = carbonSession
      .read
      .format("carbon")
      .load(path)
    result.show()

    carbonSession.close()
  }

  def write(df: DataFrame, path: String, args: Array[String]): Unit = {
    import org.apache.spark.sql.CarbonSession._

    val rootPath = new File(this.getClass.getResource("/").getPath
      + "../../../..").getCanonicalPath
    val storeLocation = s"$rootPath/examples/spark2/target/store2"
    val warehouse = s"$rootPath/examples/spark2/target/warehouse2"
    val metastoredb = s"$rootPath/examples/spark2/target/metastore_db2"

    // 1. method 1, todo
    //    val carbonSession = new CarbonSession(df.sparkSession.sparkContext)

    // 2. method 2
    val (accessKey, secretKey, endpoint) = getKeyOnPrefix(path)
    val carbonSession = SparkSession
      .builder()
      .master("local")
      .appName("carbon")
      .config(df.sparkSession.sparkContext.getConf)
      .config("spark.sql.crossJoin.enabled", "true")
      .config(Constants.ACCESS_KEY, args(0))
      .config(Constants.SECRET_KEY, args(1))
      .config(Constants.ENDPOINT, args(2))
      .getOrCreateCarbonSession(storeLocation, metastoredb)

    val rdd = carbonSession.sqlContext.sparkContext
      .parallelize(1 to 1200000, 4)
      .map { x =>
        ("city" + x % 8, "country" + x % 1103, "planet" + x % 10007, x.toString,
          (x % 16).toShort, x / 2, (x << 1).toLong, x.toDouble / 13, x.toDouble / 11)
      }.map { x =>
      Row(x._1, x._2, x._3, x._4, x._5, x._6, x._7, x._8, x._9)
    }

    val schema = StructType(
      Seq(
        StructField("city", StringType, nullable = false),
        StructField("country", StringType, nullable = false),
        StructField("planet", StringType, nullable = false),
        StructField("id", StringType, nullable = false),
        StructField("m1", ShortType, nullable = false),
        StructField("m2", IntegerType, nullable = false),
        StructField("m3", LongType, nullable = false),
        StructField("m4", DoubleType, nullable = false),
        StructField("m5", DoubleType, nullable = false)
      )
    )
    carbonSession.createDataFrame(rdd, schema)
      .write
      .format("carbon")
      .save(path)

    carbonSession.close()
  }

  def getKeyOnPrefix(path: String): (String, String, String) = {
    val endPoint = "spark.hadoop." + ENDPOINT
    if (path.startsWith(CarbonCommonConstants.S3A_PREFIX)) {
      ("spark.hadoop." + ACCESS_KEY, "spark.hadoop." + SECRET_KEY, endPoint)
    } else if (path.startsWith(CarbonCommonConstants.S3N_PREFIX)) {
      ("spark.hadoop." + CarbonCommonConstants.S3N_ACCESS_KEY,
        "spark.hadoop." + CarbonCommonConstants.S3N_SECRET_KEY, endPoint)
    } else if (path.startsWith(CarbonCommonConstants.S3_PREFIX)) {
      ("spark.hadoop." + CarbonCommonConstants.S3_ACCESS_KEY,
        "spark.hadoop." + CarbonCommonConstants.S3_SECRET_KEY, endPoint)
    } else {
      throw new Exception("Incorrect Store Path")
    }
  }
}
