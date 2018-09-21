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
    JNIEnv *jniEnv;
    jobject carbonReaderBuilderObject;
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

    jobject build();

    jobject build(char *ak, char *sk, char *endpoint);

    jboolean hasNext();

    jobjectArray readNextRow();

    jboolean close();
};
