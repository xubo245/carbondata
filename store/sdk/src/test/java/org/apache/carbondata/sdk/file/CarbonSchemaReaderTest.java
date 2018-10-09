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

package org.apache.carbondata.sdk.file;

import java.io.File;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import junit.framework.TestCase;

import org.apache.carbondata.common.exceptions.sql.InvalidLoadOptionException;
import org.apache.carbondata.core.metadata.datatype.DataTypes;
import org.apache.commons.io.FileUtils;

public class CarbonSchemaReaderTest extends TestCase {
  String path = "./testWriteFiles";

  @Before
  public void setUp() throws IOException, InvalidLoadOptionException {
    FileUtils.deleteDirectory(new File(path));

    Field[] fields = new Field[11];
    fields[0] = new Field("stringField", DataTypes.STRING);
    fields[1] = new Field("shortField", DataTypes.SHORT);
    fields[2] = new Field("intField", DataTypes.INT);
    fields[3] = new Field("longField", DataTypes.LONG);
    fields[4] = new Field("doubleField", DataTypes.DOUBLE);
    fields[5] = new Field("boolField", DataTypes.BOOLEAN);
    fields[6] = new Field("dateField", DataTypes.DATE);
    fields[7] = new Field("timeField", DataTypes.TIMESTAMP);
    fields[8] = new Field("decimalField", DataTypes.createDecimalType(8, 2));
    fields[9] = new Field("varcharField", DataTypes.VARCHAR);
    fields[10] = new Field("arrayField", DataTypes.createArrayType(DataTypes.STRING));
    Map<String, String> map = new HashMap<>();
    map.put("complex_delimiter_level_1", "#");
    CarbonWriter writer = CarbonWriter.builder()
        .outputPath(path)
        .withLoadOptions(map)
        .withCsvInput(new Schema(fields)).build();

    for (int i = 0; i < 10; i++) {
      String[] row2 = new String[]{
          "robot" + (i % 10),
          String.valueOf(i % 10000),
          String.valueOf(i),
          String.valueOf(Long.MAX_VALUE - i),
          String.valueOf((double) i / 2),
          String.valueOf(true),
          "2019-03-02",
          "2019-02-12 03:03:34",
          "12.345",
          "varchar",
          "Hello#World#From#Carbon"
      };
      writer.write(row2);
    }
    writer.close();
  }

  public void checkFieldName(Schema schema) {
    // Transform the schema
    assertEquals(schema.getFields().length, 11);
    String[] strings = new String[schema.getFields().length];
    for (int i = 0; i < schema.getFields().length; i++) {
      strings[i] = (schema.getFields())[i].getFieldName();
    }
    assert (strings[0].equalsIgnoreCase("stringField"));
    assert (strings[1].equalsIgnoreCase("shortField"));
    assert (strings[2].equalsIgnoreCase("intField"));
    assert (strings[3].equalsIgnoreCase("longField"));
    assert (strings[4].equalsIgnoreCase("doubleField"));
    assert (strings[5].equalsIgnoreCase("boolField"));
    assert (strings[6].equalsIgnoreCase("dateField"));
    assert (strings[7].equalsIgnoreCase("timeField"));
    assert (strings[8].equalsIgnoreCase("decimalField"));
    assert (strings[9].equalsIgnoreCase("varcharField"));
    assert (strings[10].equalsIgnoreCase("arrayField"));
  }

  @Test
  public void testReadSchemaFromDataFile() {
    try {
      Schema schema = CarbonSchemaReader
          .readSchemaFromFirstDataFile(path)
          .asOriginOrder();
      checkFieldName(schema);
    } catch (Throwable e) {
      e.printStackTrace();
    }
  }

  @Test
  public void testReadSchemaFromIndexFile() {
    try {
      Schema schema = CarbonSchemaReader
          .readSchemaFromFirstIndexFile(path)
          .asOriginOrder();
      checkFieldName(schema);
    } catch (Throwable e) {
      e.printStackTrace();
    }
  }

  @After
  public void tearDown() throws IOException {
    FileUtils.deleteDirectory(new File(path));
  }
}
