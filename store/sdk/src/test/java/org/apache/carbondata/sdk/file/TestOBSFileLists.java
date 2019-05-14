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

import org.apache.carbondata.core.util.path.CarbonTablePath;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.s3a.Constants;
import org.apache.hadoop.mapreduce.InputSplit;

import java.util.List;

import static org.apache.carbondata.sdk.file.utils.SDKUtil.listFiles;

public class TestOBSFileLists {
  public static void main(String[] args) throws Exception {

    Configuration conf = new Configuration(true);
    conf.set(Constants.ACCESS_KEY, args[0]);
    conf.set(Constants.SECRET_KEY, args[1]);
    conf.set(Constants.ENDPOINT, args[2]);

    String path = "s3a://manifest/carbon/manifestcarbon/binary/";
    List fileLists = listFiles(path, CarbonTablePath.CARBON_DATA_EXT, conf);

    InputSplit[] splits = CarbonReader
        .builder()
        .withHadoopConf(conf)
        .withFileLists(fileLists.subList(0, 2))
        .getSplits(true);
    System.out.println(splits.length);
  }
}
