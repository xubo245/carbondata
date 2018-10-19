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

#include <jni.h>

class CarbonWriter {
private:
    /**
    * jni env
    */
    JNIEnv *jniEnv;

    /**
     * carbonWriterBuilder object for building carbonWriter
     * it can configure some operation
     */
    jobject carbonWriterBuilderObject;

    /**
     * carbonWriter object for writing data
     */
    jobject carbonWriterObject;

    /**
     * carbon writer class
     */
    jclass carbonWriter;

    /**
     * write method id
     */
    jmethodID writeID;
public:
    /**
     * create a CarbonWriterBuilder object for building carbonWriter,
     * CarbonWriterBuilder object  can configure different parameter
     *
     * @param env JNIEnv
     * @return CarbonWriterBuilder object
     */
    jobject builder(JNIEnv *env);

    /**
     * Sets the output path of the writer builder
     *
     * @param path is the absolute path where output files are written
     * This method must be called when building CarbonWriterBuilder
     * @return updated CarbonWriterBuilder
     */
    jobject outputPath(char *path);

    /**
     * configure the schema with json style schema
     *
     * @param jsonSchema json style schema
     * @return updated CarbonWriterBuilder
     */
    jobject withCsvInput(char *jsonSchema);

    /**
    * configure parameter, including ak,sk and endpoint
    *
    * @param key key word
    * @param value value
    * @return CarbonWriterBuilder object
    */
    jobject withHadoopConf(char *key, char *value);

    /**
     * build carbonWriter object for writing data
     * it support write data from load disk
     *
     * @return carbonWriter object
     */
    jobject build();

    /**
     * Write an object to the file, the format of the object depends on the
     * implementation.
     * Note: This API is not thread safe
     */
    void write(jobject obj);

    /**
     * close the carbon Writer
     *
     * @return  boolean value
     */
    jboolean close();
};


