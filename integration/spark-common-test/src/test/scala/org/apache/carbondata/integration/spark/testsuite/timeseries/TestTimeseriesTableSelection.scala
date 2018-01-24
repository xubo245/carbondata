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

import java.sql.Timestamp

import org.apache.spark.sql.{CarbonDatasourceHadoopRelation, Row}
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.execution.datasources.LogicalRelation
import org.apache.spark.sql.hive.CarbonRelation
import org.apache.spark.sql.test.util.QueryTest
import org.scalatest.BeforeAndAfterAll


class TestTimeseriesTableSelection extends QueryTest with BeforeAndAfterAll {

  override def beforeAll: Unit = {
    sql("drop table if exists mainTable")
    sql("CREATE TABLE mainTable(mytime timestamp, name string, age int) STORED BY 'org.apache.carbondata.format'")
    sql("create datamap agg0 on table mainTable using 'preaggregate' DMPROPERTIES ('timeseries.eventTime'='mytime', 'timeseries.hierarchy'='second=1,minute=1,hour=1,day=1,month=1,year=1') as select mytime, sum(age) from mainTable group by mytime")
    sql(s"LOAD DATA LOCAL INPATH '$resourcesPath/timeseriestest.csv' into table mainTable")
  }

  test("test PreAggregate table selection 1") {
    val df = sql("select mytime from mainTable group by mytime")
    preAggTableValidator(df.queryExecution.analyzed, "maintable")
  }

  test("test PreAggregate table selection 2") {
    val df = sql("select timeseries(mytime,'hour') from mainTable group by timeseries(mytime,'hour')")
    preAggTableValidator(df.queryExecution.analyzed, "maintable_agg0_hour")
  }

  test("test PreAggregate table selection 3") {
    val df = sql("select timeseries(mytime,'milli') from mainTable group by timeseries(mytime,'milli')")
    preAggTableValidator(df.queryExecution.analyzed, "maintable")
  }

  test("test PreAggregate table selection 4") {
    val df = sql("select timeseries(mytime,'year') from mainTable group by timeseries(mytime,'year')")
    preAggTableValidator(df.queryExecution.analyzed,"maintable_agg0_year")
  }

  test("test PreAggregate table selection 5") {
    val df = sql("select timeseries(mytime,'day') from mainTable group by timeseries(mytime,'day')")
    preAggTableValidator(df.queryExecution.analyzed,"maintable_agg0_day")
  }

  test("test PreAggregate table selection 6") {
    val df = sql("select timeseries(mytime,'month') from mainTable group by timeseries(mytime,'month')")
    preAggTableValidator(df.queryExecution.analyzed,"maintable_agg0_month")
  }

  test("test PreAggregate table selection 7") {
    val df = sql("select timeseries(mytime,'minute') from mainTable group by timeseries(mytime,'minute')")
    preAggTableValidator(df.queryExecution.analyzed,"maintable_agg0_minute")
  }

  test("test PreAggregate table selection 8") {
    val df = sql("select timeseries(mytime,'second') from mainTable group by timeseries(mytime,'second')")
    preAggTableValidator(df.queryExecution.analyzed,"maintable_agg0_second")
  }

  test("test PreAggregate table selection 9") {
    val df = sql("select timeseries(mytime,'hour') from mainTable where timeseries(mytime,'hour')='x' group by timeseries(mytime,'hour')")
    preAggTableValidator(df.queryExecution.analyzed,"maintable_agg0_hour")
  }

  test("test PreAggregate table selection 10") {
    val df = sql("select timeseries(mytime,'hour') from mainTable where timeseries(mytime,'hour')='x' group by timeseries(mytime,'hour') order by timeseries(mytime,'hour')")
    preAggTableValidator(df.queryExecution.analyzed,"maintable_agg0_hour")
  }

  test("test PreAggregate table selection 11") {
    val df = sql("select timeseries(mytime,'hour'),sum(age) from mainTable where timeseries(mytime,'hour')='x' group by timeseries(mytime,'hour') order by timeseries(mytime,'hour')")
    preAggTableValidator(df.queryExecution.analyzed,"maintable_agg0_hour")
  }

  test("test PreAggregate table selection 12") {
    val df = sql("select timeseries(mytime,'hour')as hourlevel,sum(age) as sum from mainTable where timeseries(mytime,'hour')='x' group by timeseries(mytime,'hour') order by timeseries(mytime,'hour')")
    preAggTableValidator(df.queryExecution.analyzed,"maintable_agg0_hour")
  }

  test("test PreAggregate table selection 13") {
    val df = sql("select timeseries(mytime,'hour')as hourlevel,sum(age) as sum from mainTable where timeseries(mytime,'hour')='x' and name='vishal' group by timeseries(mytime,'hour') order by timeseries(mytime,'hour')")
    preAggTableValidator(df.queryExecution.analyzed,"maintable")
  }

  test("test PreAggregate table selection 14: No enum constant MILLI") {
    try {
      val df = sql(
        """
          | select timeseries(mytime,'milli')
          | from mainTable
          | group by timeseries(mytime,'milli')
          | """.stripMargin)
      preAggTableValidator(df.queryExecution.analyzed, "maintable")
      df.show()
      assert(false)
    } catch {
      case e: Exception =>
        assert(e.getMessage.contains("No enum constant org.apache.carbondata.core.preagg.TimeSeriesFunctionEnum.MILLI"))
    }
  }

  test("test PreAggregate table selection 15: timeseries(mytime,'hour') match") {
    val df = sql(
      """
        | select timeseries(mytime,'hour')
        | from mainTable
        | where timeseries(mytime,'hour')='2016-02-23 01:00:00'
        | group by timeseries(mytime,'hour')
        | """.stripMargin)
    preAggTableValidator(df.queryExecution.analyzed, "maintable_agg0_hour")
    checkAnswer(df, Row(Timestamp.valueOf("2016-02-23 01:00:00.0")))
  }

  test("test PreAggregate table selection 16: timeseries(mytime,'hour') not match") {
    val df = sql(
      """
        | select timeseries(mytime,'hour')
        | from mainTable
        | where timeseries(mytime,'hour')='2016-02-23 01:01:00'
        | group by timeseries(mytime,'hour')
        | """.stripMargin)
    preAggTableValidator(df.queryExecution.analyzed, "maintable_agg0_hour")
    checkExistence(df, false, "2016-02-23 01:00:00", "2016-02-23 01:01:00")
  }

  test("test PreAggregate table selection 17: timeseries(mytime,'minute') match") {
    checkExistence(sql("select * from mainTable"), true, "2016-02-23 01:01:30", "2016-02-23 01:02:40")
    checkExistence(sql("select * from mainTable"), false, "2016-02-23 01:02:00", "2016-02-23 01:01:00")
    val df = sql("select timeseries(mytime,'minute') from mainTable group by timeseries(mytime,'minute')")
    preAggTableValidator(df.queryExecution.analyzed, "maintable_agg0_minute")
    checkExistence(df, true, "2016-02-23 01:02:00", "2016-02-23 01:01:00")
    checkAnswer(df,
      Seq(Row(Timestamp.valueOf("2016-02-23 01:02:00.0")),
        Row(Timestamp.valueOf("2016-02-23 01:01:00.0"))))

    val df2 = sql(
      """
        | select
        |   timeseries(mytime,'minute')as minutelevel,
        |   sum(age) as sum
        | from mainTable
        | where timeseries(mytime,'minute')='2016-02-23 01:01:00'
        | group by timeseries(mytime,'minute')
        | order by timeseries(mytime,'minute')
        | """.stripMargin)
    preAggTableValidator(df2.queryExecution.analyzed, "maintable_agg0_minute")
    checkAnswer(df2, Seq(Row(Timestamp.valueOf("2016-02-23 01:01:00"), 60)))
  }

  test("test PreAggregate table selection 18: timeseries(mytime,'minute') not match pre aggregate table") {
    val df = sql(
      """
        | select
        |   timeseries(mytime,'minute')as minutelevel,
        |   sum(age) as sum
        | from mainTable
        | where timeseries(mytime,'minute')='2016-02-23 01:01:00' and name='vishal'
        | group by timeseries(mytime,'minute')
        | order by timeseries(mytime,'minute')
        | """.stripMargin)
    checkAnswer(df, Seq(Row(Timestamp.valueOf("2016-02-23 01:01:00"), 10)))
    preAggTableValidator(df.queryExecution.analyzed, "maintable")
  }

  test("test PreAggregate table selection 19: select with many group by and one filter") {
    val df = sql(
      """
        | select
        |   timeseries(mytime,'year') as yearLevel,
        |   timeseries(mytime,'month') as monthLevel,
        |   timeseries(mytime,'day') as dayLevel,
        |   timeseries(mytime,'hour') as hourLevel,
        |   timeseries(mytime,'minute') as minuteLevel,
        |   timeseries(mytime,'second') as secondLevel,
        |   sum(age) as sum
        | from mainTable
        | where timeseries(mytime,'minute')='2016-02-23 01:01:00'
        | group by
        |   timeseries(mytime,'year'),
        |   timeseries(mytime,'month'),
        |   timeseries(mytime,'day'),
        |   timeseries(mytime,'hour'),
        |   timeseries(mytime,'minute'),
        |   timeseries(mytime,'second')
        | order by
        |   timeseries(mytime,'year'),
        |   timeseries(mytime,'month'),
        |   timeseries(mytime,'day'),
        |   timeseries(mytime,'hour'),
        |   timeseries(mytime,'minute'),
        |   timeseries(mytime,'second')
        | """.stripMargin)

    checkExistence(df, true,
      "2016-01-01 00:00:00",
      "2016-02-01 00:00:00",
      "2016-02-23 01:00:00",
      "2016-02-23 01:01:00",
      "2016-02-23 01:01:50",
      "30"
    )

    // TODO: to be discussed
    //    preAggTableValidator(df.queryExecution.analyzed, "maintable_agg0")
  }

  test("test PreAggregate table selection 20: select with many group by and many filter, don't check the preaggregate table") {
    val df = sql(
      """
        | select
        |   timeseries(mytime,'year') as yearLevel,
        |   timeseries(mytime,'month') as monthLevel,
        |   timeseries(mytime,'day') as dayLevel,
        |   timeseries(mytime,'hour') as hourLevel,
        |   timeseries(mytime,'minute') as minuteLevel,
        |   timeseries(mytime,'second') as secondLevel,
        |   sum(age) as sum
        | from mainTable
        | where
        |   timeseries(mytime,'second')='2016-02-23 01:01:50' and
        |   timeseries(mytime,'minute')='2016-02-23 01:01:00' and
        |   timeseries(mytime,'hour')='2016-02-23 01:00:00' and
        |   timeseries(mytime,'month')='2016-02-01 00:00:00' and
        |   timeseries(mytime,'year')='2016-01-01 00:00:00'
        | group by
        |   timeseries(mytime,'year'),
        |   timeseries(mytime,'month'),
        |   timeseries(mytime,'day'),
        |   timeseries(mytime,'hour'),
        |   timeseries(mytime,'minute'),
        |   timeseries(mytime,'second')
        | order by
        |   timeseries(mytime,'year'),
        |   timeseries(mytime,'month'),
        |   timeseries(mytime,'day'),
        |   timeseries(mytime,'hour'),
        |   timeseries(mytime,'minute'),
        |   timeseries(mytime,'second')
        | """.stripMargin)

    checkExistence(df, true,
      "2016-01-01 00:00:00",
      "2016-02-01 00:00:00",
      "2016-02-23 01:00:00",
      "2016-02-23 01:01:00",
      "2016-02-23 01:01:50",
      "30"
    )

  }

  test("test PreAggregate table selection 21: filter >=") {
    val df = sql(
      """
        | select
        |   timeseries(mytime,'minute') as minuteLevel,
        |   sum(age) as sum
        | from mainTable
        | where timeseries(mytime,'minute')>='2016-02-23 01:01:00'
        | group by
        |   timeseries(mytime,'minute')
        | order by
        |   timeseries(mytime,'minute')
        | """.stripMargin)

    checkAnswer(df,
      Seq(Row(Timestamp.valueOf("2016-02-23 01:01:00"), 60),
        Row(Timestamp.valueOf("2016-02-23 01:02:00"), 140)))

    preAggTableValidator(df.queryExecution.analyzed, "maintable_agg0_minute")
  }

  test("test PreAggregate table selection 22: filter >") {
    val df = sql(
      """
        | select
        |   timeseries(mytime,'minute') as minuteLevel,
        |   sum(age) as sum
        | from mainTable
        | where timeseries(mytime,'minute')>'2016-02-23 01:01:00'
        | group by
        |   timeseries(mytime,'minute')
        | order by
        |   timeseries(mytime,'minute')
        | """.stripMargin)

    checkAnswer(df,
      Seq(Row(Timestamp.valueOf("2016-02-23 01:02:00"), 140)))

    preAggTableValidator(df.queryExecution.analyzed, "maintable_agg0_minute")
  }


  test("test PreAggregate table selection 23: filter >") {
    val df = sql(
      """
        | select
        |   timeseries(mytime,'minute') as minuteLevel,
        |   sum(age) as sum
        | from mainTable
        | where timeseries(mytime,'minute')>'2016-02-23 01:02:00'
        | group by
        |   timeseries(mytime,'minute')
        | order by
        |   timeseries(mytime,'minute')
        | """.stripMargin)

    checkAnswer(df,
      Seq.empty)

    preAggTableValidator(df.queryExecution.analyzed, "maintable_agg0_minute")
  }

  test("test PreAggregate table selection 24: filter >=") {
    val df = sql(
      """
        | select
        |   timeseries(mytime,'minute') as minuteLevel,
        |   sum(age) as sum
        | from mainTable
        | where timeseries(mytime,'minute')>='2016-02-23 01:00:01'
        | group by
        |   timeseries(mytime,'minute')
        | order by
        |   timeseries(mytime,'minute')
        | """.stripMargin)

    checkAnswer(df,
      Seq(Row(Timestamp.valueOf("2016-02-23 01:01:00"), 60),
        Row(Timestamp.valueOf("2016-02-23 01:02:00"), 140)))

    preAggTableValidator(df.queryExecution.analyzed, "maintable_agg0_minute")
  }

  test("test PreAggregate table selection 25: filter <=") {
    val df = sql(
      """
        | select
        |   timeseries(mytime,'minute') as minuteLevel,
        |   sum(age) as sum
        | from mainTable
        | where timeseries(mytime,'minute')<='2016-02-23 01:02:00'
        | group by
        |   timeseries(mytime,'minute')
        | order by
        |   timeseries(mytime,'minute')
        | """.stripMargin)

    checkAnswer(df,
      Seq(Row(Timestamp.valueOf("2016-02-23 01:01:00"), 60),
        Row(Timestamp.valueOf("2016-02-23 01:02:00"), 140)))

    preAggTableValidator(df.queryExecution.analyzed, "maintable_agg0_minute")
  }

  test("test PreAggregate table selection 26: filter <=") {
    val df = sql(
      """
        | select
        |   timeseries(mytime,'minute') as minuteLevel,
        |   sum(age) as sum
        | from mainTable
        | where timeseries(mytime,'minute')<='2016-02-23 01:01:09'
        | group by
        |   timeseries(mytime,'minute')
        | order by
        |   timeseries(mytime,'minute')
        | """.stripMargin)

    checkAnswer(df,
      Seq(Row(Timestamp.valueOf("2016-02-23 01:01:00"), 60)))

    preAggTableValidator(df.queryExecution.analyzed, "maintable_agg0_minute")
  }

  test("test PreAggregate table selection 27: filter <=") {
    val df = sql(
      """
        | select
        |   timeseries(mytime,'minute') as minuteLevel,
        |   sum(age) as sum
        | from mainTable
        | where timeseries(mytime,'minute')<='2016-02-23 01:00:09'
        | group by
        |   timeseries(mytime,'minute')
        | order by
        |   timeseries(mytime,'minute')
        | """.stripMargin)

    checkAnswer(df, Seq.empty)

    preAggTableValidator(df.queryExecution.analyzed, "maintable_agg0_minute")
  }

  test("test PreAggregate table selection 28: filter <") {
    val df = sql(
      """
        | select
        |   timeseries(mytime,'minute') as minuteLevel,
        |   sum(age) as sum
        | from mainTable
        | where timeseries(mytime,'minute')<'2016-02-23 01:02:00'
        | group by
        |   timeseries(mytime,'minute')
        | order by
        |   timeseries(mytime,'minute')
        | """.stripMargin)

    checkAnswer(df,
      Seq(Row(Timestamp.valueOf("2016-02-23 01:01:00"), 60)))

    preAggTableValidator(df.queryExecution.analyzed, "maintable_agg0_minute")
  }

  test("test PreAggregate table selection 29: filter <") {
    val df = sql(
      """
        | select
        |   timeseries(mytime,'minute') as minuteLevel,
        |   sum(age) as sum
        | from mainTable
        | where timeseries(mytime,'minute')<'2016-02-23 01:04:00'
        | group by
        |   timeseries(mytime,'minute')
        | order by
        |   timeseries(mytime,'minute')
        | """.stripMargin)

    checkAnswer(df,
      Seq(Row(Timestamp.valueOf("2016-02-23 01:01:00"), 60),
        Row(Timestamp.valueOf("2016-02-23 01:02:00"), 140)))

    preAggTableValidator(df.queryExecution.analyzed, "maintable_agg0_minute")
  }

  test("test PreAggregate table selection 30: filter <") {
    val df = sql(
      """
        | select
        |   timeseries(mytime,'minute') as minuteLevel,
        |   sum(age) as sum
        | from mainTable
        | where timeseries(mytime,'minute')<'2016-02-23 01:01:00'
        | group by
        |   timeseries(mytime,'minute')
        | order by
        |   timeseries(mytime,'minute')
        | """.stripMargin)

    checkAnswer(df, Seq.empty)

    preAggTableValidator(df.queryExecution.analyzed, "maintable_agg0_minute")
  }

  test("test PreAggregate table selection 31: filter < and >") {
    val df = sql(
      """
        | select
        |   timeseries(mytime,'minute') as minuteLevel,
        |   sum(age) as sum
        | from mainTable
        | where timeseries(mytime,'minute')<'2016-02-23 01:04:00' and timeseries(mytime,'minute')>'2016-02-23 01:00:00'
        | group by
        |   timeseries(mytime,'minute')
        | order by
        |   timeseries(mytime,'minute')
        | """.stripMargin)

    checkAnswer(df,
      Seq(Row(Timestamp.valueOf("2016-02-23 01:01:00"), 60),
        Row(Timestamp.valueOf("2016-02-23 01:02:00"), 140)))

    preAggTableValidator(df.queryExecution.analyzed, "maintable_agg0_minute")
  }

  test("test PreAggregate table selection 32: filter < and >") {
    val df = sql(
      """
        | select
        |   timeseries(mytime,'minute') as minuteLevel,
        |   sum(age) as sum
        | from mainTable
        | where timeseries(mytime,'minute')<'2016-02-23 01:04:00' and timeseries(mytime,'minute')>'2016-02-23 01:01:00'
        | group by
        |   timeseries(mytime,'minute')
        | order by
        |   timeseries(mytime,'minute')
        | """.stripMargin)

    checkAnswer(df, Seq(Row(Timestamp.valueOf("2016-02-23 01:02:00"), 140)))

    preAggTableValidator(df.queryExecution.analyzed, "maintable_agg0_minute")
  }

  test("test PreAggregate table selection 33: filter <= and >=") {
    val df = sql(
      """
        | select
        |   timeseries(mytime,'minute') as minuteLevel,
        |   sum(age) as sum
        | from mainTable
        | where timeseries(mytime,'minute')<='2016-02-23 01:02:00' and timeseries(mytime,'minute')>='2016-02-23 01:01:00'
        | group by
        |   timeseries(mytime,'minute')
        | order by
        |   timeseries(mytime,'minute')
        | """.stripMargin)

    checkAnswer(df,
      Seq(Row(Timestamp.valueOf("2016-02-23 01:01:00"), 60),
        Row(Timestamp.valueOf("2016-02-23 01:02:00"), 140)))

    preAggTableValidator(df.queryExecution.analyzed, "maintable_agg0_minute")
  }

  test("test PreAggregate table selection 34: filter < and >=") {
    val df = sql(
      """
        | select
        |   timeseries(mytime,'minute') as minuteLevel,
        |   sum(age) as sum
        | from mainTable
        | where timeseries(mytime,'minute')<'2016-02-23 01:02:00' and timeseries(mytime,'minute')>='2016-02-23 01:01:00'
        | group by
        |   timeseries(mytime,'minute')
        | order by
        |   timeseries(mytime,'minute')
        | """.stripMargin)

    checkAnswer(df, Seq(Row(Timestamp.valueOf("2016-02-23 01:01:00"), 60)))

    preAggTableValidator(df.queryExecution.analyzed, "maintable_agg0_minute")
  }

  test("test PreAggregate table selection 35: filter < and >=") {
    val df = sql(
      """
        | select
        |   timeseries(mytime,'minute') as minuteLevel,
        |   sum(age) as sum
        | from mainTable
        | where timeseries(mytime,'minute')<'2016-02-23 01:01:00' and timeseries(mytime,'minute')>='2016-02-23 01:01:00'
        | group by
        |   timeseries(mytime,'minute')
        | order by
        |   timeseries(mytime,'minute')
        | """.stripMargin)

    checkAnswer(df, Seq.empty)

    preAggTableValidator(df.queryExecution.analyzed, "maintable_agg0_minute")
  }

  test("test PreAggregate table selection 36: filter many column") {
    val df = sql(
      """
        | select
        |   timeseries(mytime,'minute') as minuteLevel,
        |   sum(age) as sum
        | from mainTable
        | where
        |   timeseries(mytime,'minute')<'2016-02-23 01:02:00' and
        |   timeseries(mytime,'hour')>='2016-02-23 01:00:00' and
        |   name='vishal'
        | group by
        |   timeseries(mytime,'minute')
        | order by
        |   timeseries(mytime,'minute')
        | """.stripMargin)

    checkAnswer(df, Seq(Row(Timestamp.valueOf("2016-02-23 01:01:00"), 10)))
  }

  test("test PreAggregate table selection 37: filter < and >=, avg") {
    val df = sql(
      """
        | select
        |   timeseries(mytime,'minute') as minuteLevel,
        |   avg(age) as avg
        | from mainTable
        | where timeseries(mytime,'minute')<'2016-02-23 01:02:00' and timeseries(mytime,'minute')>='2016-02-23 01:01:00'
        | group by
        |   timeseries(mytime,'minute')
        | order by
        |   timeseries(mytime,'minute')
        | """.stripMargin)

    checkAnswer(df, Seq(Row(Timestamp.valueOf("2016-02-23 01:01:00"), 20.0)))

    // TODO: to be discussed
    //    preAggTableValidator(df.queryExecution.analyzed, "maintable_agg0_minute")
  }

  // TODO: support max
  ignore("test PreAggregate table selection 38: filter < and >=, max") {
    val df = sql(
      """
        | select
        |   timeseries(mytime,'second') as secondLevel,
        |   max(age) as maxValue
        | from mainTable
        | where timeseries(mytime,'second')<'2016-02-23 01:02:00' and timeseries(mytime,'second')>='2016-02-23 01:01:00'
        | group by
        |   timeseries(mytime,'second')
        | order by
        |   timeseries(mytime,'second')
        | """.stripMargin)

    df.show()
    checkAnswer(df, Seq(Row(Timestamp.valueOf("2016-02-23 01:01:50"), 30)))

    // TODO: to be discussed
    //    preAggTableValidator(df.queryExecution.analyzed, "maintable_agg0_minute")
  }

  ignore("test PreAggregate table selection 39: filter < and >=, min") {
    val df = sql(
      """
        | select
        |   timeseries(mytime,'second') as secondLevel,
        |   min(age) as minValue
        | from mainTable
        | where timeseries(mytime,'second')<'2016-02-23 01:02:00' and timeseries(mytime,'second')>='2016-02-23 01:01:00'
        | group by
        |   timeseries(mytime,'second')
        | order by
        |   timeseries(mytime,'second')
        | """.stripMargin)

    df.show()
    checkAnswer(df, Seq(Row(Timestamp.valueOf("2016-02-23 01:01:30"), 10)))

    // TODO: to be discussed
    //    preAggTableValidator(df.queryExecution.analyzed, "maintable_agg0_minute")
  }

  def preAggTableValidator(plan: LogicalPlan, actualTableName: String) : Unit ={
    var isValidPlan = false
    plan.transform {
      // first check if any preaTable1 scala function is applied it is present is in plan
      // then call is from create preaTable1regate table class so no need to transform the query plan
      case ca:CarbonRelation =>
        if (ca.isInstanceOf[CarbonDatasourceHadoopRelation]) {
          val relation = ca.asInstanceOf[CarbonDatasourceHadoopRelation]
          if(relation.carbonTable.getTableName.equalsIgnoreCase(actualTableName)) {
            isValidPlan = true
          }
        }
        ca
      case logicalRelation:LogicalRelation =>
        if(logicalRelation.relation.isInstanceOf[CarbonDatasourceHadoopRelation]) {
          val relation = logicalRelation.relation.asInstanceOf[CarbonDatasourceHadoopRelation]
          if(relation.carbonTable.getTableName.equalsIgnoreCase(actualTableName)) {
            isValidPlan = true
          }
        }
        logicalRelation
    }
    if(!isValidPlan) {
      assert(false)
    } else {
      assert(true)
    }
  }

  override def afterAll: Unit = {
    dropDataMaps("maintable", "agg0_second", "agg0_hour", "agg0_day", "agg0_month", "agg0_year")
    sql("drop table if exists mainTable")
  }
}
