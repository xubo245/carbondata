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

package org.apache.carbondata.examples.sdk;

import org.apache.carbondata.core.metadata.datatype.DataTypes;
import org.apache.carbondata.sdk.file.*;
import org.apache.commons.io.FileUtils;
import org.apache.hadoop.conf.Configuration;
import org.junit.Assert;
import org.junit.Test;

import java.io.File;
import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import static org.apache.hadoop.fs.s3a.Constants.ACCESS_KEY;
import static org.apache.hadoop.fs.s3a.Constants.ENDPOINT;
import static org.apache.hadoop.fs.s3a.Constants.SECRET_KEY;

/**
 * multi-thread Test suite for {@link CSVCarbonWriter}
 */
public class ConcurrentSdkWriterTest {

  private static final int recordsPerItr = 2;
  private static final int numOfThreads = 16;
  private static final int fileNum = 15;
  private static final String ak = "";
  private static final String sk = "";
  private static final String endPoint = "obs.cn-north-1.myhwclouds.com";


//  @Ignore
//  public void testWriteFiles() throws IOException {
//    String path = "./testWriteFiles";
//    FileUtils.deleteDirectory(new File(path));
//
//    Field[] fields = new Field[2];
//    fields[0] = new Field("name", DataTypes.STRING);
//    fields[1] = new Field("age", DataTypes.INT);
//
//
//
//    ExecutorService executorService = Executors.newFixedThreadPool(numOfThreads);
//    try {
//      CarbonWriterBuilder builder = CarbonWriter.builder()
//          .outputPath(path);
//      CarbonWriter writer =
//          builder.buildThreadSafeWriterForCSVInput(new Schema(fields), numOfThreads, TestUtil.configuration);
//      // write in multi-thread
//      for (int i = 0; i < numOfThreads; i++) {
//        executorService.submit(new WriteLogic(writer));
//      }
//      executorService.shutdown();
//      executorService.awaitTermination(2, TimeUnit.HOURS);
//      writer.close();
//    } catch (Exception e) {
//      e.printStackTrace();
//      Assert.fail(e.getMessage());
//    }
//
//    // read the files and verify the count
//    CarbonReader reader;
//    try {
//      reader = CarbonReader
//          .builder(path, "_temp")
//          .projection(new String[]{"name", "age"})
//          .build(new Configuration(false));
//      int i = 0;
//      while (reader.hasNext()) {
//        Object[] row = (Object[]) reader.readNextRow();
//        i++;
//      }
//      Assert.assertEquals(i, numOfThreads * recordsPerItr);
//      reader.close();
//    } catch (InterruptedException e) {
//      e.printStackTrace();
//      Assert.fail(e.getMessage());
//    }
//
//    FileUtils.deleteDirectory(new File(path));
//  }

  @Test
  public void testConcurrentWriteFiles() throws IOException {
//    String pathString = "/Users/xubo/Desktop/xubo/git/carbondata1/store/sdk/testWriteFilesString/";
//    String pathArray = "/Users/xubo/Desktop/xubo/git/carbondata1/store/sdk/testWriteFilesArray/";

    String pathString = "s3a://sdk/concurrent/testWriteFilesString/";
    String pathArray =  "s3a://sdk/concurrent/testWriteFilesArray/";

    FileUtils.deleteDirectory(new File(pathString));
    FileUtils.deleteDirectory(new File(pathArray));

    Field[] fieldsString = new Field[2];
    fieldsString[0] = new Field("name", DataTypes.STRING);
    fieldsString[1] = new Field("age", DataTypes.INT);


    Field[] fieldsArray = new Field[3];
    fieldsArray[0] = new Field("name", DataTypes.STRING);
    fieldsArray[1] = new Field("age", DataTypes.INT);
    fieldsArray[2] = new Field("arrayField", DataTypes.createArrayType(DataTypes.STRING));


    ExecutorService executorService = Executors.newFixedThreadPool(numOfThreads);
    try {

      Configuration conf = new Configuration();
      conf.set(ACCESS_KEY,ak);
      conf.set(SECRET_KEY,sk);
      conf.set(ENDPOINT,endPoint);
      // write in multi-thread
      for (int i = 0; i < fileNum * 2; i++) {
        System.out.println("file num:" + i/2);
        String time = new SimpleDateFormat("yyyyMMddHHmmssSSS").format(new Date());
        short numOfThread = 1;
        if (i % 2 == 0) {
          CarbonWriter writerString = CarbonWriter
              .builder()
              .outputPath(pathString + time)
              .buildThreadSafeWriterForCSVInput(new Schema(fieldsString),numOfThread, conf);
          executorService.submit(new WriteLogic(writerString, pathString, new Schema(fieldsString)));
        } else {
          Map<String, String> map = new HashMap<>();
          map.put("complex_delimiter_level_1", "#");
          CarbonWriter writerArray = CarbonWriter
              .builder()
              .outputPath(pathArray + time)
              .withLoadOptions(map)
              .buildThreadSafeWriterForCSVInput(new Schema(fieldsArray), numOfThread,conf);
          executorService.submit(new WriteLogic(writerArray, pathArray, new Schema(fieldsArray)));
        }
      }
      executorService.shutdown();
      executorService.awaitTermination(1, TimeUnit.MINUTES);
    } catch (Exception e) {
      e.printStackTrace();
      Assert.fail(e.getMessage());
    }

  }

  class WriteLogic implements Runnable {
    CarbonWriter writer;
    String path;
    Schema schema;

    public WriteLogic(CarbonWriter writer, String path, Schema schema) {
      this.writer = writer;
      this.path = path;
      this.schema = schema;
    }

    @Override
    public void run() {
      try {
        System.out.println("line number:" + schema.getFields().length);
        if (schema.getFields().length < 3) {
          for (int i = 0; i < recordsPerItr*2; i++) {
//            if(i==recordsPerItr/2){
//              try {
//                Thread.sleep(3000);
//              } catch (InterruptedException e) {
//                e.printStackTrace();
//              }
//            }
            writer.write(new String[]{"robot" + (i % 10), String.valueOf(i),
                String.valueOf((double) i / 2)});
          }
        } else {
          for (int i = 0; i < recordsPerItr; i++) {
//            if(i==recordsPerItr/2){
//              try {
//                Thread.sleep(3000);
//              } catch (InterruptedException e) {
//                e.printStackTrace();
//              }
//            }
            writer.write(new String[]{"robot" + (i % 10), String.valueOf(i),
                "Hello#World#From#Carbon"});
          }
        }

      } catch (IOException e) {
        e.printStackTrace();
        Assert.fail(e.getMessage());
      }
      try {
        System.out.println("close:"+schema.getFields().length);
        writer.close();
      } catch (IOException e) {
        e.printStackTrace();
        Assert.fail(e.getMessage());
      }

    }
  }

}
