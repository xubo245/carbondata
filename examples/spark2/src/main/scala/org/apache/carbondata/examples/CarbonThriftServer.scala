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
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.hive.thriftserver.HiveThriftServer2
import org.slf4j.{Logger, LoggerFactory}
import org.apache.carbondata.common.logging.LogServiceFactory
import org.apache.carbondata.core.constants.CarbonCommonConstants
import org.apache.carbondata.core.util.CarbonProperties

import scala.tools.nsc.io.Path

object CarbonThriftServer {

  def main(args: Array[String]): Unit = {

    import org.apache.spark.sql.CarbonSession._

    val sparkConf = new SparkConf(loadDefaults = true)

    val logger: Logger = LoggerFactory.getLogger(this.getClass)
    if (args.length != 1 && args.length != 4) {
      logger.error("Usage: java CarbonThriftServer  storePath <access-key> <secret-key>" +
        "[s3-endpoint] master")
      System.exit(0)
    }

    val (accessKey, secretKey, endpoint) = getKeyOnPrefix(args(0))

    val builder = SparkSession
      .builder()
      .config(sparkConf)
      .appName("Carbon Thrift Server(uses CarbonSession)")
      .config(accessKey, args(1))
      .config(secretKey, args(2))
      .config(endpoint, getS3EndPoint(args))
      .enableHiveSupport()
      .master(getSparkMaster(args))

    if (!sparkConf.contains("carbon.properties.filepath")) {
      val sparkHome = System.getenv.get("SPARK_HOME")
      if (null != sparkHome) {
        val file = new File(sparkHome + '/' + "conf" + '/' + "carbon.properties")
        if (file.exists()) {
          builder.config("carbon.properties.filepath", file.getCanonicalPath)
          System.setProperty("carbon.properties.filepath", file.getCanonicalPath)
        }
      }
    } else {
      System.setProperty("carbon.properties.filepath", sparkConf.get("carbon.properties.filepath"))
    }

    val storePath = if (args.length > 0) args.head else null

    val spark = builder.getOrCreateCarbonSession()
    val warmUpTime = CarbonProperties.getInstance().getProperty("carbon.spark.warmUpTime", "5000")
    try {
      Thread.sleep(Integer.parseInt(warmUpTime))
    } catch {
      case e: Exception =>
        val LOG = LogServiceFactory.getLogService(this.getClass.getCanonicalName)
        LOG.error(s"Wrong value for carbon.spark.warmUpTime $warmUpTime " +
                  "Using default Value and proceeding")
        Thread.sleep(5000)
    }

//    val rootPath = new File(this.getClass.getResource("/").getPath
//      + "../../../..").getCanonicalPath
//    val path = s"$rootPath/examples/spark2/src/main/resources/data1.csv"
//    operation(spark,args,path = path)
    HiveThriftServer2.startWithContext(spark.sqlContext)
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

  def getS3EndPoint(args: Array[String]): String = {
    if (args.length >= 4 && args(3).contains(".com")) args(3)
    else ""
  }

  def getSparkMaster(args: Array[String]): String = {
    if (args.length == 5) args(4)
    else if (args(3).contains("spark:") || args(3).contains("mesos:")) args(3)
    else "local"
  }

  def operation(spark:SparkSession,args:Array[String],path: String): Unit ={
    spark.sql("Drop table if exists carbon_table")

    spark.sql(
      s"""
         | CREATE TABLE if not exists carbon_table(
         | shortField SHORT,
         | intField INT,
         | bigintField LONG,
         | doubleField DOUBLE,
         | stringField STRING,
         | timestampField TIMESTAMP,
         | decimalField DECIMAL(18,2),
         | dateField DATE,
         | charField CHAR(5),
         | floatField FLOAT
         | )
         | STORED BY 'carbondata'
         | LOCATION '${ args(0) }'
         | TBLPROPERTIES('SORT_COLUMNS'='', 'DICTIONARY_INCLUDE'='dateField, charField')
       """.stripMargin)

    spark.sql(
      s"""
         | LOAD DATA LOCAL INPATH '$path'
         | INTO TABLE carbon_table
         | OPTIONS('HEADER'='true')
       """.stripMargin)

    spark.sql(
      s"""
         | SELECT *
         | FROM carbon_table
      """.stripMargin).show()

    spark.sql(
      s"""
         | LOAD DATA LOCAL INPATH '$path'
         | INTO TABLE carbon_table
         | OPTIONS('HEADER'='true')
       """.stripMargin)

    spark.sql(
      s"""
         | SELECT *
         | FROM carbon_table
      """.stripMargin).show()

  }
}
