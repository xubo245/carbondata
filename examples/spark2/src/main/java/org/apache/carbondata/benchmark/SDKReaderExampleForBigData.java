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

package org.apache.carbondata.benchmark;

import java.io.File;
import java.io.FilenameFilter;
import java.io.IOException;
import java.sql.Timestamp;
import java.util.HashMap;
import java.util.Map;
import java.util.Random;

import org.apache.hadoop.conf.Configuration;

import org.apache.carbondata.common.exceptions.sql.InvalidLoadOptionException;
import org.apache.carbondata.core.constants.CarbonCommonConstants;
import org.apache.carbondata.core.util.CarbonProperties;
import org.apache.carbondata.sdk.file.*;

/**
 * Test SDK read example for big data
 */
public class SDKReaderExampleForBigData {
  public static void main(String[] args) throws InterruptedException, InvalidLoadOptionException, IOException {
    System.out.println("start to read data");
    String path = "../../../../Downloads/carbon-data-big";
    if (args.length > 0) {
      path = args[0];
    }
    double num = 1000000000.0;
    String originPath = "../../../../Downloads/carbon-data";
    String newPath = "../../../../Downloads/carbon-data-big";
    boolean writeNewData = false;
    if (writeNewData) {
      extendData(originPath, newPath);
    }

    Configuration conf = new Configuration();
    if (args.length > 3) {
      conf.set("fs.s3a.access.key", args[1]);
      conf.set("fs.s3a.secret.key", args[2]);
      conf.set("fs.s3a.endpoint", args[3]);
    }
    readNextBatchRow(path, num, conf, 100000, 100000);
    readNextRow(path, num, conf, 100000);
  }

  public static void readNextRow(String path, double num, Configuration conf, int printNum) {
    System.out.println("readNextRow");
    try {
      // Read data
      Long startTime = System.nanoTime();
      CarbonReader reader = CarbonReader
          .builder(path, "_temp")
          .withHadoopConf(conf)
          .build();

      Long startReadTime = System.nanoTime();
      System.out.println("build time is " + (startReadTime - startTime) / num);
      int i = 0;
      while (reader.hasNext()) {
        Object[] data = (Object[]) reader.readNextRow();
        i++;
        if (i % printNum == 0) {
          Long point = System.nanoTime();
          System.out.print(i + ": time is " + (point - startReadTime) / num
              + " s, speed is " + (i / ((point - startReadTime) / num)) + " records/s \t");
          for (int j = 0; j < data.length; j++) {
            System.out.print(data[j] + "\t\t");
          }
          System.out.println();
        }
      }
      Long endReadTime = System.nanoTime();
      System.out.println("total lines is " + i + ", build time is " + (startReadTime - startTime) / num
          + " s, \ttotal read time is " + (endReadTime - startReadTime) / num
          + " s, \taverage speed is " + (i / ((endReadTime - startReadTime) / num))
          + " records/s.");
      reader.close();
    } catch (Throwable e) {
      e.printStackTrace();
    }
  }

  /**
   * read next batch row
   *
   * @param path     data path
   * @param num      number for time
   * @param conf     configuration
   * @param batch    batch size
   * @param printNum print number for each batch
   */
  public static void readNextBatchRow(String path, double num, Configuration conf, int batch, int printNum) {
    System.out.println("readNextBatchRow");
    try {
      // Read data
      Long startTime = System.nanoTime();
      CarbonReader reader = CarbonReader
          .builder(path, "_temp")
          .withHadoopConf(conf)
          .withBatch(batch)
          .build();

      Long startReadTime = System.nanoTime();
      Long startBatchReadTime = startReadTime;
      System.out.println("build time is " + (startBatchReadTime - startTime) / num);
      int i = 0;
      long startHasNext = System.nanoTime();
      while (reader.hasNext()) {
        Long endHasNext = System.nanoTime();

        Object[] batchRow = reader.readNextBatchRow();
        for (int k = 0; k < batchRow.length; k++) {
          Object[] data = (Object[]) batchRow[k];
          i++;
          if (i > 0 && i % printNum == 0) {
            Long point = System.nanoTime();
            System.out.print(i + ": time is " + (point - startBatchReadTime) / num
                + " s, \tspeed is " + (printNum / ((point - startBatchReadTime) / num))
                + " records/s, \thasNext time is " + (endHasNext - startHasNext) / num + " s\t");
            for (int j = 0; j < data.length; j++) {
              System.out.print(data[j] + "\t\t");
            }
            System.out.println();
            startBatchReadTime = System.nanoTime();
          }
        }
        startHasNext = System.nanoTime();
      }
      Long endReadTime = System.nanoTime();
      System.out.println("total lines is " + i + ", build time is " + (startReadTime - startTime) / num
          + " s, \ttotal read time is " + (endReadTime - startReadTime) / num
          + " s, \taverage speed is " + (i / ((endReadTime - startReadTime) / num))
          + " records/s.");
      reader.close();
    } catch (Throwable e) {
      e.printStackTrace();
    }
  }

  public static Schema readSchema(String path) throws IOException {
    File[] dataFiles = new File(path).listFiles(new FilenameFilter() {
      @Override
      public boolean accept(File dir, String name) {
        if (name == null) {
          return false;
        }
        return name.endsWith("carbondata");
      }
    });
    if (dataFiles == null || dataFiles.length < 1) {
      throw new RuntimeException("Carbon index file not exists.");
    }
    Schema schema = CarbonSchemaReader
        .readSchemaInDataFile(dataFiles[0].getAbsolutePath())
        .asOriginOrder();
    return schema;
  }

  /**
   * extend data
   * read origin path data and generate new data in new path,
   * the new data is bigger than origin data
   *
   * @param originPath origin path of data
   * @param newPath    new path of data
   * @throws IOException
   * @throws InterruptedException
   * @throws InvalidLoadOptionException
   */
  public static void extendData(String originPath, String newPath)
      throws IOException, InterruptedException, InvalidLoadOptionException {
    CarbonProperties.getInstance()
        .addProperty(CarbonCommonConstants.UNSAFE_WORKING_MEMORY_IN_MB, "2048")
        .addProperty(CarbonCommonConstants.IN_MEMORY_FOR_SORT_DATA_IN_MB, "4048");

    Map<String, String> map = new HashMap<>();
    map.put("complex_delimiter_level_1", "#");

    // Read data
    CarbonReader reader = CarbonReader
        .builder(originPath, "_temp")
        .build();

    int i = 1;
    int num = 100000;
    int ram = num;
    Random random = new Random();
    Object[] objects = new Object[3000];
    while (reader.hasNext()) {
      Object[] row = (Object[]) reader.readNextRow();
      if (!(row[0] == null
          || row[1] == null
          || row[2] == null
          || row[0].equals("null")
          || row[1].equals("null")
          || row[2].equals("null"))) {
        System.out.print(i + ":\t");

        objects[i - 1] = row;

        i++;
      }
      System.out.println();
    }
    reader.close();

    CarbonWriterBuilder builder = CarbonWriter.builder()
        .outputPath(newPath)
        .withLoadOptions(map)
        .withBlockSize(128)
        .withCsvInput(readSchema(originPath));
    CarbonWriter writer = builder.build();

    for (int index = 0; index < i - 2; index++) {
      System.out.println(index);
      Object[] row = (Object[]) objects[index];
      String[] strings = new String[row.length];
      for (int j = 2; j < row.length; j++) {
        if (row[j] instanceof Long) {
          strings[j] = new Timestamp((Long) row[j] / 1000).toString();
        } else {
          strings[j] = (String) row[j];
        }
      }
      for (int k = 0; k < num; k++) {
        strings[0] = "Source" + random.nextInt(ram) + row[0];
        strings[1] = "Target" + random.nextInt(ram) + row[1];
        writer.write(strings);
      }

      if (index > 1 && index % 400 == 0) {
        System.out.println("start to close and build again");
        for (int a = 0; a < strings.length; a++) {
          System.out.print(strings[a] + "\t\t\t\t");
        }
        writer.close();
        Thread.sleep(2000);
        writer = builder.build();
      }
    }

    writer.close();
  }

}
