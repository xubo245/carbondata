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

import org.apache.carbondata.sdk.file.*;
import org.apache.hadoop.conf.Configuration;

import static org.apache.hadoop.fs.s3a.Constants.ACCESS_KEY;
import static org.apache.hadoop.fs.s3a.Constants.ENDPOINT;
import static org.apache.hadoop.fs.s3a.Constants.SECRET_KEY;

/**
 * Example fo CarbonReader with close method
 * After readNextRow of CarbonReader, User should close the reader,
 * otherwise main will continue run some time
 */
public class CarbonReaderConcurrenctTest {
    private static final String ak = "";
    private static final String sk = "";
    private static final String endPoint = "obs.cn-north-1.myhwclouds.com";

    public static void main(String[] args) {
//        String pathString = "/Users/xubo/Desktop/xubo/git/carbondata1/store/sdk/testWriteFilesString";
//        String pathArray = "/Users/xubo/Desktop/xubo/git/carbondata1/store/sdk/testWriteFilesArray";

        String pathString = "s3a://sdk/concurrent/testWriteFilesString/";
        String pathArray =  "s3a://sdk/concurrent/testWriteFilesArray/";

        Configuration conf = new Configuration();
        conf.set(ACCESS_KEY,ak);
        conf.set(SECRET_KEY,sk);
        conf.set(ENDPOINT,endPoint);
        try {
            // Read data
            int i = 1;
            CarbonReader reader = CarbonReader
                .builder(pathString, "_temp")
                .projection(new String[] {"name","age"})
                .setAccessKey(ak)
                .setSecretKey(sk)
                .setEndPoint(endPoint)
                .build(conf);
            while (reader.hasNext()) {
                Object[] row = (Object[]) reader.readNextRow();
                if (i < 20) {
//                System.out.println("column length:"+row.length);
                    System.out.print(i + ":\t");
                    for (int j = 0; j < row.length; j++) {
                        System.out.print(row[j] + "\t");
                    }
                    System.out.println();
                }
                i++;
            }
            System.out.println("total:"+(i-1));
            reader.close();

            System.out.println();
            // Read data
            CarbonReader reader2 = CarbonReader
                .builder(pathArray, "_temp")
                .projection(new String[] {"name","age","arrayField"})
                .setAccessKey(ak)
                .setSecretKey(sk)
                .setEndPoint(endPoint)
                .build(conf);
            i = 1;
            while (reader2.hasNext()) {
                Object[] row = (Object[]) reader2.readNextRow();
//                System.out.println("column length:"+row.length);
                if(i<20){
                    System.out.print(i + ":\t");
                    for (int j = 0; j < row.length; j++) {
                        if (row[j] instanceof Object[]) {
                            Object[] array = (Object[]) row[j];
                            for (int k = 0; k < array.length; k++) {
                                System.out.print(array[k] + "#");
                            }
                            System.out.print("\t");
                        } else {
                            System.out.print(row[j] + "\t");
                        }
                    }
                    System.out.println();
                }
                i++;
            }
            System.out.println("total:"+(i-1));
            reader2.close();

        } catch (Throwable e) {
            e.printStackTrace();
            System.out.println(e.getMessage());
        }
    }
}
