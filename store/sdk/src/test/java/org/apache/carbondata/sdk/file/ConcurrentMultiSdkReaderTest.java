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

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.s3a.Constants;

import org.junit.Assert;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.*;

import static org.apache.carbondata.sdk.file.utils.SDKUtil.getSplitList;

/**
 * multi-thread Test suite for {@link CarbonReader}
 */
public class ConcurrentMultiSdkReaderTest {
  public static void main(String[] args) throws InterruptedException {

    long startTime = System.nanoTime();
    short numThreads = 1;
    long count = 0;
    //    String path = "/Users/xubo/Desktop/xubo/data/VOCdevkit/carbon/voc";
    String path = "/Users/xubo/Desktop/xubo/git/carbondata1/store/sdk/target/flowers";

    // Concurrent Reading
    ExecutorService executorService = Executors.newFixedThreadPool(numThreads);
    try {
      List<ReadLogic> tasks = new ArrayList<>();
      List<Future<Long>> results;

      Configuration conf = new Configuration(true);
      if (args.length > 3) {
        conf.set(Constants.ACCESS_KEY, args[1]);
        conf.set(Constants.SECRET_KEY, args[2]);
        conf.set(Constants.ENDPOINT, args[3]);
      }

      Object[] splitList = getSplitList(path, ".carbondata", numThreads, conf);
      for (int i = 0; i < splitList.length; i++) {
        tasks.add(new ReadLogic((List) splitList[i]));
      }

      long start = System.currentTimeMillis();
      results = executorService.invokeAll(tasks);
      for (Future result_i : results) {
        count += (long) result_i.get();
      }
      long end = System.currentTimeMillis();
      System.out.println("[Parallel read] Time: " + (end - start) / 1000.0 + " s");
      System.out.println(count);
    } catch (Exception e) {
      e.printStackTrace();
      Assert.fail(e.getMessage());
    } finally {
      executorService.shutdown();
      executorService.awaitTermination(10, TimeUnit.MINUTES);
      long endTime = System.nanoTime();
      System.out.println("total time is " + (endTime - startTime) / 1000000000.0);
    }
  }

  static class ReadLogic implements Callable<Long> {
    List fileList;

    ReadLogic(List fileList) {
      this.fileList = fileList;
    }

    @Override
    public Long call() throws IOException, InterruptedException {

      CarbonReader reader = CarbonReader.builder().withFileLists(this.fileList)
          .filterEqual("txtcontent", "daisy")
          .build();
      long count = 0;
      try {
        while (reader.hasNext()) {
          Object[] rows = reader.readNextBatchRow();
          for (int i = 0; i < rows.length; i++) {
            Object[] row = (Object[]) rows[i];
            for (int j = 0; j < row.length; j++) {
              Object column = row[j];
            }
            count += 1;
          }
        }
      } catch (Exception e) {
        e.printStackTrace();
        Assert.fail(e.getMessage());
      } finally {
        reader.close();
      }
      return count;
    }
  }

}