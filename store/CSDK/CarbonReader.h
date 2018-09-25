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

#ifndef TM_SCALA_CARBONREADER_H
#define TM_SCALA_CARBONREADER_H


#include <jni.h>
#include <string>

#endif //TM_SCALA_CARBONREADER_H

class CarbonReader {
public:
    /**
     * jni env
     */
    JNIEnv *jniEnv;

    /**
     * carbonReaderBuilder object for building carbonReader
     * it can configure some operation
     */
    jobject carbonReaderBuilderObject;

    /**
     * carbonReader object for reading data
     */
    jobject carbonReaderObject;

    /**
     * create a CarbonReaderBuilder object for building carbonReader,
     * CarbonReaderBuilder object  can configure different parameter
     *
     * @param env JNIEnv
     * @param path data store path
     * @param tableName table name
     * @return CarbonReaderBuilder object
     */
    jobject builder(JNIEnv *env, char *path, char *tableName);

    /**
     * build carbonReader object for reading data
     * it support read data from load disk
     *
     * @return carbonReader object
     */
    jobject build();

    /**
     * build carbon reader with simple ak, sk and endpoint value
     *
     * @param ak user's access key value
     * @param sk user's secret key value
     * @param endpoint  user's endpoint
     * @return carbonReader object
     */
    jobject build(char *ak, char *sk, char *endpoint);

    /**
     *  build carbon reader with argument vector
     *  it support multiple parameter
     *  like: key=value
     *  for example: fs.s3a.access.key=XXXX, XXXX is user's access key value
     *
     * @param argc argument counter
     * @param argv argument vector
     * @return carbonReader object
     */
    jobject build(int argc, char *argv[]);

    /**
     * Whether it has next row data
     *
     * @return boolean value, if it has next row, return true. if it hasn't next row, return false.
     */
    jboolean hasNext();

    /**
     * read next row from data
     *
     * @return object array of one row
     */
    jobjectArray readNextRow();

    /**
     * close the carbon reader
     *
     * @return  boolean value
     */
    jboolean close();
};
