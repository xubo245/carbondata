package org.apache.carbondata.spark.testsuite.datetype

import java.io.File
import org.apache.carbondata.core.datastore.impl.FileFactory
import org.apache.carbondata.core.metadata.datatype.DataTypes
import org.apache.carbondata.sdk.file.{CarbonReader, CarbonWriter, Field, Schema}
import org.apache.spark.sql.test.util.QueryTest
import org.scalatest.BeforeAndAfterAll

class FloatTest extends QueryTest with BeforeAndAfterAll {

    test("test float") {
        val path = FileFactory.getPath(warehouse + "/sdk1").toString
        FileFactory.deleteAllFilesOfDir(new File(warehouse + "/sdk1"))
        sql("drop table if exists carbon_float")
        var fields: Array[Field] = new Array[Field](1)
        // same column name, but name as boolean type
        fields(0) = new Field("b", DataTypes.FLOAT)

        try {
            val builder = CarbonWriter.builder()
            val writer =
                builder.outputPath(path)
                        .uniqueIdentifier(System.nanoTime()).withBlockSize(2)
                        .withCsvInput(new Schema(fields)).writtenBy("SparkCarbonDataSourceTest").build()

            var i = 0
            while (i < 1) {
                val array = Array[String](
                    "2147483648.1")
                writer.write(array)
                i += 1
            }
            writer.close()

            val reader = CarbonReader.builder(path, "_temp").build
            i = 0
            var floatValueSDK: Float = 0
            while (i < 20 && reader.hasNext) {
                val row = reader.readNextRow.asInstanceOf[Array[AnyRef]]
                println("SDK float value is: " + row(0))
                floatValueSDK = row(0).asInstanceOf[Float]
                i += 1
            }
            reader.close()

            sql("create table carbon_float(floatField float) stored as carbondata")
            sql("insert into carbon_float values('2147483648.1')")
            val df = sql("Select * from carbon_float").collect()
            println("CarbonSession float value is: " + df(0))
            assert(df(0).equals(floatValueSDK))
        } catch {
            case ex: Exception => throw new RuntimeException(ex)
        } finally {
            sql("drop table if exists carbon_float")
            FileFactory.deleteAllFilesOfDir(new File(warehouse + "/sdk1"))
        }
    }


}
