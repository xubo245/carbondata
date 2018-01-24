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
package org.apache.carbondata.integration.spark.testsuite.timeseries

import org.apache.spark.sql.catalyst.analysis.TableAlreadyExistsException
import org.apache.spark.sql.test.util.QueryTest
import org.scalatest.{BeforeAndAfterAll, BeforeAndAfterEach}
import org.apache.carbondata.spark.exception.MalformedCarbonCommandException
import org.apache.spark.sql.AnalysisException

class TestTimeSeriesCreateTable extends QueryTest with BeforeAndAfterAll with BeforeAndAfterEach{

  override def beforeAll: Unit = {
    sql("drop table if exists mainTable")
    sql("CREATE TABLE mainTable(dataTime timestamp, name string, city string, age int) STORED BY 'org.apache.carbondata.format'")
    sql("create datamap agg0 on table mainTable using 'preaggregate' DMPROPERTIES ('timeseries.eventTime'='dataTime', 'timeseries.hierarchy'='second=1,hour=1,day=1,month=1,year=1') as select dataTime, sum(age) from mainTable group by dataTime")
  }

  override def afterEach(): Unit = {
    dropDataMaps("mainTable", "agg1_second", "agg1_hour", "agg1_day", "agg1_month", "agg1_year")
  }

  test("test timeseries create table Zero") {
    checkExistence(sql("DESCRIBE FORMATTED mainTable_agg0_second"), true, "maintable_agg0_second")
    sql("drop datamap agg0_second on table mainTable")
  }

  test("test timeseries create table One") {
    checkExistence(sql("DESCRIBE FORMATTED mainTable_agg0_hour"), true, "maintable_agg0_hour")
    sql("drop datamap agg0_hour on table mainTable")
  }

  test("test timeseries create table two") {
    checkExistence(sql("DESCRIBE FORMATTED maintable_agg0_day"), true, "maintable_agg0_day")
    sql("drop datamap agg0_day on table mainTable")
  }

  test("test timeseries create table three") {
    checkExistence(sql("DESCRIBE FORMATTED mainTable_agg0_month"), true, "maintable_agg0_month")
    sql("drop datamap agg0_month on table mainTable")
  }

  test("test timeseries create table four") {
    checkExistence(sql("DESCRIBE FORMATTED mainTable_agg0_year"), true, "maintable_agg0_year")
    sql("drop datamap agg0_year on table mainTable")
  }

  test("test timeseries create table five") {
    try {
      sql(
        "create datamap agg0 on table mainTable using 'preaggregate' DMPROPERTIES ('timeseries.eventTime'='dataTime', 'timeseries.hierarchy'='sec=1,hour=1,day=1,month=1,year=1') as select dataTime, sum(age) from mainTable group by dataTime")
      assert(false)
    } catch {
      case _:Exception =>
        assert(true)
    }
  }

  test("test timeseries create table Six") {
    try {
      sql(
        "create datamap agg0 on table mainTable using 'preaggregate' DMPROPERTIES ('timeseries.eventTime'='dataTime', 'timeseries.hierarchy'='hour=2') as select dataTime, sum(age) from mainTable group by dataTime")
      assert(false)
    } catch {
      case _:Exception =>
        assert(true)
    }
  }

  test("test timeseries create table seven") {
    try {
      sql(
        "create datamap agg0 on table mainTable using 'preaggregate' DMPROPERTIES ('timeseries.eventTime'='dataTime', 'timeseries.hierarchy'='hour=1,day=1,year=1,month=1') as select dataTime, sum(age) from mainTable group by dataTime")
      assert(false)
    } catch {
      case _:Exception =>
        assert(true)
    }
  }

  test("test timeseries create table Eight") {
    try {
      sql(
        "create datamap agg0 on table mainTable using 'preaggregate' DMPROPERTIES ('timeseries.eventTime'='name', 'timeseries.hierarchy'='hour=1,day=1,year=1,month=1') as select name, sum(age) from mainTable group by name")
      assert(false)
    } catch {
      case _:Exception =>
        assert(true)
    }
  }

  test("test timeseries create table Nine") {
    try {
      sql(
        "create datamap agg0 on table mainTable using 'preaggregate' DMPROPERTIES ('timeseries.eventTime'='dataTime', 'timeseries.hierarchy'='hour=1,day=1,year=1,month=1') as select name, sum(age) from mainTable group by name")
      assert(false)
    } catch {
      case _:Exception =>
        assert(true)
    }
  }

  test("test timeseries create table 10: Not supported hierarchy type") {
    try {
      sql(
        """create datamap agg1 on table mainTable using 'preaggregate'
          |DMPROPERTIES (
          |   'timeseries.eventTime'='dataTime',
          |   'timeseries.hierarchy'='sec=1,hour=1,day=1,month=1,year=1')
          |as select dataTime, sum(age) from mainTable
          |group by dataTime
          |""".stripMargin)
      assert(false)
    } catch {
      case e: MalformedCarbonCommandException =>
        assert(e.getMessage.contains("Not supported hierarchy type"))
    }
  }

  test("test timeseries create table 11: Table or view already exists in database") {
    try {
      sql(
        """create datamap agg1 on table mainTable using 'preaggregate'
          |DMPROPERTIES (
          |   'timeseries.eventTime'='dataTime',
          |   'timeseries.hierarchy'='second=1,hour=1,day=1,month=1,year=1')
          |as select dataTime, sum(age) from mainTable
          |group by dataTime
          |""".stripMargin)
      sql(
        """create datamap agg1 on table mainTable using 'preaggregate'
          |DMPROPERTIES (
          |   'timeseries.eventTime'='dataTime',
          |   'timeseries.hierarchy'='second=1,hour=1,day=1,month=1,year=1')
          |as select dataTime, sum(age) from mainTable
          |group by dataTime
          |""".stripMargin)
      assert(false)
    } catch {
      case e: TableAlreadyExistsException =>
        assert(e.getMessage.contains("Table or view"))
        assert(e.getMessage.contains("already exists in database"))
        assert(true)
    }
  }

  test("test timeseries create table 12: hierarchy type with space") {
    try {
      sql(
        """create datamap agg1 on table mainTable using 'preaggregate'
          |DMPROPERTIES (
          |   'timeseries.eventTime'='dataTime',
          |   'timeseries.hierarchy'='second= 1,hour=1,day=1,month=1,year=1')
          |as select dataTime, sum(age) from mainTable
          |group by dataTime
          |""".stripMargin)
      assert(false)
    } catch {
      case e: MalformedCarbonCommandException =>
        assert(e.getMessage.contains("Unsupported Value for hierarchy:second= 1"))
        assert(true)
    }
  }

  test("test timeseries create table 13: don't support hour=2") {
    try {
      sql(
        """create datamap agg1 on table mainTable using 'preaggregate'
          |DMPROPERTIES (
          |   'timeseries.eventTime'='dataTime',
          |   'timeseries.hierarchy'='hour=2')
          |as select dataTime, sum(age) from mainTable
          |group by dataTime
          |""".stripMargin)
      assert(false)
    } catch {
      case e: MalformedCarbonCommandException =>
        assert(e.getMessage.contains("Unsupported Value for hierarchy:hour=2"))
    } finally {
      checkExistence(sql("show tables"), false, "maintable_agg1_hour")
    }
  }

  test("test timeseries create table 14: Table or view 'maintable_agg1_hour' already exists in database") {
    try {
      sql(
        """create datamap agg1 on table mainTable using 'preaggregate'
          |DMPROPERTIES (
          |   'timeseries.eventTime'='dataTime',
          |   'timeseries.hierarchy'='hour=1,day=1,year=1,month=1')
          |as select dataTime, sum(age) from mainTable
          |group by dataTime
          |""".stripMargin)
      sql(
        """create datamap agg1 on table mainTable using 'preaggregate'
          |DMPROPERTIES (
          |   'timeseries.eventTime'='dataTime',
          |   'timeseries.hierarchy'='hour=1,day=1,year=1,month=1')
          |as select dataTime, sum(age) from mainTable
          |group by dataTime
          |""".stripMargin)
      assert(false)
    } catch {
      case e: TableAlreadyExistsException =>
        assert(e.getMessage.contains("Table or view 'maintable_agg1_hour' already exists in database"))
    }
  }

  test("test timeseries create table 15: support hour=1,day=1,year=1,month=1") {
    try {
      sql(
        """create datamap agg1 on table mainTable
          |using 'preaggregate'
          |DMPROPERTIES (
          |   'timeseries.eventTime'='dataTime',
          |   'timeseries.hierarchy'='hour=1,day=1,year=1,month=1')
          |as select dataTime, sum(age) from mainTable
          |group by dataTime
          |""".stripMargin)
      assert(true)
    } catch {
      case _: Exception =>
        assert(false)
    } finally {
      checkExistence(sql("show tables"), true, "maintable_agg1_hour")
    }
  }

  test("test timeseries create table 16: don't support create timeseries table on non timestamp column") {
    try {
      sql(
        """create datamap agg1 on table mainTable
          |using 'preaggregate'
          |DMPROPERTIES (
          |   'timeseries.eventTime'='name',
          |   'timeseries.hierarchy'='hour=1,day=1,year=1,month=1')
          |as select name, sum(age) from mainTable
          |group by name
          |""".stripMargin)
      assert(false)
    } catch {
      case e: MalformedCarbonCommandException =>
        assert(e.getMessage.contains("Timeseries event time is only supported on Timestamp column"))
    }
  }

  test("test timeseries create table 17: Time series column dataTime does not exists in select") {
    try {
      sql(
        """create datamap agg1 on table mainTable
          |using 'preaggregate'
          |DMPROPERTIES (
          |   'timeseries.eventTime'='dataTime',
          |   'timeseries.hierarchy'='hour=1,day=1,year=1,month=1')
          |as select name, sum(age) from mainTable
          |group by name
          |""".stripMargin)
      assert(false)
    } catch {
      case e: MalformedCarbonCommandException =>
        assert(e.getMessage.contains("Time series column dataTime does not exists in select"))
    }
  }

  test("test timeseries create table 18: support day=1,year=1,month=1") {
    try {
      sql(
        """create datamap agg1 on table mainTable
          | using 'preaggregate'
          | DMPROPERTIES (
          |    'timeseries.eventTime'='dataTime',
          |    'timeseries.hierarchy'='day=1,year=1,month=1')
          | as select dataTime, sum(age) from mainTable
          | group by dataTime
          |""".stripMargin)
      checkExistence(sql("show tables"), false, "maintable_agg1_hour")
      checkExistence(sql("show tables"), true, "maintable_agg1_day", "maintable_agg1_month", "maintable_agg1_year")
    } catch {
      case _: Exception =>
        assert(false)
    }
  }

  test("test timeseries create table 19: support month=1,year=1") {
    try {
      sql(
        """create datamap agg1 on table mainTable
          |using 'preaggregate'
          |DMPROPERTIES (
          |   'timeseries.eventTime'='dataTime',
          |   'timeseries.hierarchy'='month=1,year=1')
          |as select dataTime, sum(age) from mainTable
          |group by dataTime
          |""".stripMargin)
      checkExistence(sql("show tables"), false, "maintable_agg1_hour", "maintable_agg1_day")
      checkExistence(sql("show tables"), true, "maintable_agg1_month", "maintable_agg1_year")
    } catch {
      case _: Exception =>
        assert(false)
    }
  }

  test("test timeseries create table 20: support year=1") {
    try {
      sql(
        """create datamap agg1 on table mainTable
          |using 'preaggregate'
          |DMPROPERTIES (
          |   'timeseries.eventTime'='dataTime',
          |   'timeseries.hierarchy'='year=1')
          |as select dataTime, sum(age) from mainTable
          |group by dataTime
          |""".stripMargin)
      checkExistence(sql("show tables"), false, "maintable_agg1_hour")
      checkExistence(sql("show tables"), false, "maintable_agg1_day")
      checkExistence(sql("show tables"), false, "maintable_agg1_month")
      checkExistence(sql("show tables"), true, "maintable_agg1_year")
    } catch {
      case _: Exception =>
        assert(false)
    }
  }

  test("test timeseries create table 21: don't support year=1,month=1") {
    try {
      sql(
        """create datamap agg1 on table mainTable
          |using 'preaggregate'
          |DMPROPERTIES (
          |   'timeseries.eventTime'='dataTime',
          |   'timeseries.hierarchy'='year=1,month=1')
          |as select dataTime, sum(age) from mainTable
          |group by dataTime""".stripMargin)
      assert(false)
    } catch {
      case e: MalformedCarbonCommandException =>
        assert(e.getMessage.contains("year=1,month=1 is in wrong order"))
    } finally {
      checkExistence(sql("show tables"), false, "maintable_agg1_year")
    }
  }

  // TODO: to be discussed
  test("test timeseries create table 22: don't support month=1, year=1") {
    try {
      sql(
        "create datamap agg1 on table mainTable using 'preaggregate' DMPROPERTIES ('timeseries.eventTime'='dataTime', 'timeseries.hierarchy'='month=1, year=1') as select dataTime, sum(age) from mainTable group by dataTime")
      checkExistence(sql("show tables"), false, "maintable_agg1_hour", "maintable_agg1_day")
      checkExistence(sql("show tables"), true, "maintable_agg1_month", "maintable_agg1_year")
    } catch {
      case e: Exception =>
        assert(e.getMessage.contains("Not supported hierarchy type:  year"))
    }
  }

  test("test timeseries create table 23: support month=1") {
    try {
      sql(
        """create datamap agg1 on table mainTable
          | using 'preaggregate'
          | DMPROPERTIES (
          |   'timeseries.eventTime'='dataTime',
          |   'timeseries.hierarchy'='month=1')
          | as select dataTime, sum(age) from mainTable
          | group by dataTime
          |""".stripMargin)
      checkExistence(sql("show tables"), false, "maintable_agg1_hour", "maintable_agg1_day", "maintable_agg1_year")
      checkExistence(sql("show tables"), true, "maintable_agg1_month")
    } catch {
      case _: Exception =>
        assert(false)
    }
  }

  test("test timeseries create table 24: support day=1") {
    try {
      sql(
        """create datamap agg1 on table mainTable
          | using 'preaggregate'
          | DMPROPERTIES (
          |   'timeseries.eventTime'='dataTime',
          |   'timeseries.hierarchy'='day=1')
          | as select dataTime, sum(age) from mainTable
          | group by dataTime
          |""".stripMargin)
      checkExistence(sql("show tables"), true, "maintable_agg1_day")
      checkExistence(sql("show tables"), false, "maintable_agg1_hour", "maintable_agg1_month", "maintable_agg1_year")
    } catch {
      case _: Exception =>
        assert(false)
    }
  }

  test("test timeseries create table 25: support hour=1") {
    try {
      sql(
        """create datamap agg1 on table mainTable
          | using 'preaggregate'
          | DMPROPERTIES (
          |   'timeseries.eventTime'='dataTime',
          |   'timeseries.hierarchy'='hour=1')
          | as select dataTime, sum(age) from mainTable
          | group by dataTime
          |""".stripMargin)
      checkExistence(sql("show tables"), true, "maintable_agg1_hour")
      checkExistence(sql("show tables"), false, "maintable_agg1_day", "maintable_agg1_month", "maintable_agg1_year")
    } catch {
      case _: Exception =>
        assert(false)
    }
  }

  test("test timeseries create table 26: support second=1") {
    try {
      sql(
        """create datamap agg1 on table mainTable
          | using 'preaggregate'
          | DMPROPERTIES (
          |   'timeseries.eventTime'='dataTime',
          |   'timeseries.hierarchy'='second=1')
          | as select dataTime, sum(age) from mainTable
          | group by dataTime
          |""".stripMargin)
      checkExistence(sql("show tables"), true, "maintable_agg1_second")
      checkExistence(sql("show tables"), false,
        "maintable_agg1_hour", "maintable_agg1_day", "maintable_agg1_month", "maintable_agg1_year")
    } catch {
      case _: Exception =>
        assert(false)
    }
  }

  test("test timeseries create table 27: support SECOND=1") {
    try {
      sql(
        """create datamap agg1 on table mainTable
          | using 'preaggregate'
          | DMPROPERTIES (
          |   'timeseries.eventTime'='dataTime',
          |   'timeseries.hierarchy'='SECOND=1')
          | as select dataTime, sum(age) from mainTable
          | group by dataTime
          |""".stripMargin)
      checkExistence(sql("show tables"), true, "maintable_agg1_second")
      checkExistence(sql("show tables"), false,
        "maintable_agg1_hour", "maintable_agg1_day", "maintable_agg1_month", "maintable_agg1_year")
    } catch {
      case _: Exception =>
        assert(false)
    }
  }

  test("test timeseries create table 28: support second=1,year=1") {
    try {
      sql(
        """create datamap agg1 on table mainTable
          | using 'preaggregate'
          | DMPROPERTIES (
          |   'timeseries.eventTime'='dataTime',
          |   'timeseries.hierarchy'='second=1,year=1')
          | as select dataTime, sum(age) from mainTable
          | group by dataTime
          |""".stripMargin)
      checkExistence(sql("show tables"), true, "maintable_agg1_second", "maintable_agg1_year")
      checkExistence(sql("show tables"), false,
        "maintable_agg1_hour", "maintable_agg1_day", "maintable_agg1_month")
    } catch {
      case _: Exception =>
        assert(false)
    }
  }

  test("test timeseries create table 29: test different data type") {
    sql("drop table if exists dataTable")
    sql(
      s"""
         | CREATE TABLE dataTable(
         | shortField SHORT,
         | booleanField BOOLEAN,
         | intField INT,
         | bigintField LONG,
         | doubleField DOUBLE,
         | stringField STRING,
         | decimalField DECIMAL(18,2),
         | charField CHAR(5),
         | floatField FLOAT,
         | dataTime timestamp
         | )
         | STORED BY 'carbondata'
       """.stripMargin)

    try {
      sql(
        """create datamap agg1 on table dataTable
          | using 'preaggregate'
          | DMPROPERTIES (
          |   'timeseries.eventTime'='dataTime',
          |   'timeseries.hierarchy'='second=1,year=1')
          | as select
          |   dataTime,
          |   sum(intField),
          |   shortField,
          |   booleanField,
          |   intField,
          |   bigintField,
          |   doubleField,
          |   stringField,
          |   decimalField,
          |   charField,
          |   floatField
          | from dataTable
          | group by
          |   dataTime,
          |   shortField,
          |   booleanField,
          |   intField,
          |   bigintField,
          |   doubleField,
          |   stringField,
          |   decimalField,
          |   charField,
          |   floatField
          |""".stripMargin)
      checkExistence(sql("show tables"), true, "datatable_agg1_second", "datatable_agg1_year")
      checkExistence(sql("show tables"), false,
        "datatable_agg1_hour", "datatable_agg1_day", "datatable_agg1_month")
    } catch {
      case _: Exception =>
        assert(false)
    } finally {
      sql("drop table if exists dataTable")
    }
  }

  test("test timeseries create table 30: test data map name") {
    try {
      sql(
        """create datamap agg1 on table mainTable
          |using 'preaggregate'
          |DMPROPERTIES (
          |   'timeseries.eventTime'='dataTime',
          |   'timeseries.hierarchy'='month=1,year=1')
          |as select dataTime, sum(age) from mainTable
          |group by dataTime
          |""".stripMargin)
      checkExistence(sql("show datamap on table mainTable"), true, "agg1_month", "agg1_year")
      checkExistence(sql("show datamap on table maintable"), true, "agg1_month", "agg1_year")
      checkExistence(sql("desc formatted maintable_agg1_month"), true, "maintable_age_sum")
      checkExistence(sql("desc formatted mainTable_agg1_month"), true, "maintable_age_sum")
    } catch {
      case _: Exception =>
        assert(false)
    }
  }

  test("test timeseries create table 31: timeseries.eventTime is null") {
    try {
      sql(
        """create datamap agg1 on table mainTable
          |using 'preaggregate'
          |DMPROPERTIES (
          |   'timeseries.eventTime'='',
          |   'timeseries.hierarchy'='hour=1,day=1,year=1,month=1')
          |as select name, sum(age) from mainTable
          |group by name
          |""".stripMargin)
      assert(false)
    } catch {
      case _: NullPointerException =>
        assert(true)
    }
  }

  test("test timeseries create table 32: table not exists") {
    try {
      sql(
        """create datamap agg1 on table mainTable
          |using 'preaggregate'
          |DMPROPERTIES (
          |   'timeseries.eventTime'='dataTime',
          |   'timeseries.hierarchy'='month=1,year=1')
          |as select dataTime, sum(age) from mainTableNo
          |group by dataTime
          |""".stripMargin)
      assert(false)
    } catch {
      case e: AnalysisException =>
        assert(e.getMessage.contains("Table or view not found: maintableno"))
    }
  }

  test("test timeseries create table 33: timeseries.hierarchy is null") {
    try {
      sql(
        """create datamap agg1 on table mainTable
          |using 'preaggregate'
          |DMPROPERTIES (
          |   'timeseries.eventTime'='dataTime',
          |   'timeseries.hierarchy'='')
          |as select dataTime, sum(age) from mainTable
          |group by dataTime
          |""".stripMargin)
      assert(false)
    } catch {
      case e: MalformedCarbonCommandException =>
        assert(e.getMessage.contains("Not supported hierarchy type"))
    }
  }

  override def afterAll: Unit = {
    dropTable("mainTable")
  }
}
