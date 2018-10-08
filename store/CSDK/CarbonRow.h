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

class CarbonRow {
public:
    jobject carbonRow;
    /**
     * jni env
     */
    JNIEnv *jniEnv;

    /**
     * Constructor and express the carbon row result
     *
     * @param env JNI env
     * @param jo CarbonRow object
     */
    CarbonRow(JNIEnv *env, jobject jo);

    /**
     * print result
     *
     * @param ordinal the data index of carbonRow
     */
    void print(int ordinal);

    /**
     * the length of CarbonRow object
     *
     * @return
     */
    int getLength();

    /**
     * get data type name of ordinal data
     *
     * @param ordinal the data index of carbonRow
     * @return data type name
     */
    char *getDataTypeName(int ordinal);

    /**
     * get element type name of ordinal data
     * child schema data type name
     * for example: return STRING if it's Array<String> in java
     *
     * @param ordinal the data index of carbonRow
     * @return element type name
     */
    char *getArrayElementTypeName(int ordinal);

    /**
     * get short data type data by ordinal
     *
     * @param ordinal the data index of carbonRow
     * @return short data type data
     */
    short getShort(int ordinal);

    /**
     * get int data type data by ordinal
     *
     * @param ordinal the data index of carbonRow
     * @return int data type data
     */
    int getInt(int ordinal);

    /**
     * get long data type data by ordinal
     *
     * @param ordinal the data index of carbonRow
     * @return  long data type data
     */
    long getLong(int ordinal);

    /**
     * get double data type data by ordinal
     *
     * @param ordinal the data index of carbonRow
     * @return  double data type data
     */
    double getDouble(int ordinal);

    /**
     * get float data type data by ordinal
     *
     * @param ordinal the data index of carbonRow
     * @return float data type data
     */
    float getFloat(int ordinal);

    /**
     * get boolean data type data by ordinal
     *
     * @param ordinal the data index of carbonRow
     * @return jboolean data type data
     */
    jboolean getBoolean(int ordinal);

    /**
     *  get decimal data type data by ordinal
     * JNI don't support Decimal, so carbon convert decimal to string
     *
     * @param ordinal the data index of carbonRow
     * @return string data type data
     */
    char *getDecimal(int ordinal);

    /**
     * get string data type data by ordinal
     *
     * @param ordinal the data index of carbonRow
     * @return string data type data
     */
    char *getString(int ordinal);

    /**
     * get varchar data type data by ordinal
     * JNI don't support varchar, so carbon convert decimal to string
     *
     * @param ordinal the data index of carbonRow
     * @return string data type data
     */
    char *getVarchar(int ordinal);

    /**
     * get array<T> data type data by ordinal
     *
     * @param ordinal the data index of carbonRow
     * @return jobjectArray data type data
     */
    jobjectArray getArray(int ordinal);
};
