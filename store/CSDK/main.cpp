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

#include <stdio.h>
#include <jni.h>
#include <stdlib.h>
#include <iostream>
#include <unistd.h>
#include "CarbonReader.h"
#include "CarbonRow.h"

using namespace std;

JavaVM *jvm;

/**
 * init jvm
 *
 * @return
 */
JNIEnv *initJVM() {
    JNIEnv *env;
    JavaVMInitArgs vm_args;
    int parNum = 3;
    int res;
    JavaVMOption options[parNum];

    options[0].optionString = "-Djava.compiler=NONE";
    options[1].optionString = "-Djava.class.path=../../sdk/target/carbondata-sdk.jar";
    options[2].optionString = "-verbose:jni";
    vm_args.version = JNI_VERSION_1_8;
    vm_args.nOptions = parNum;
    vm_args.options = options;
    vm_args.ignoreUnrecognized = JNI_FALSE;

    res = JNI_CreateJavaVM(&jvm, (void **) &env, &vm_args);
    if (res < 0) {
        fprintf(stderr, "\nCan't create Java VM\n");
        exit(1);
    }

    return env;
}

/**
 * test read data from local disk, without projection
 *
 * @param env  jni env
 * @return
 */
bool readFromLocalWithoutProjection(JNIEnv *env) {

    CarbonReader carbonReaderClass;
    carbonReaderClass.builder(env, "../resources/carbondata");
    carbonReaderClass.build();

    printf("\nRead data from local  without projection:\n");

    CarbonRow carbonRow(env);
    while (carbonReaderClass.hasNext()) {
        jobject row = carbonReaderClass.readNextCarbonRow();
        carbonRow.setCarbonRow(row);
        printf("%s\t", carbonRow.getString(0));
        printf("%d\t", carbonRow.getInt(1));
        printf("%ld\t", carbonRow.getLong(2));
        printf("%s\t", carbonRow.getVarchar(3));
        jobjectArray jobjectArray1 = carbonRow.getArray(4);
        jsize length = env->GetArrayLength(jobjectArray1);
        int j = 0;
        for (j = 0; j < length; j++) {
            jobject element = env->GetObjectArrayElement(jobjectArray1, j);
            char *str = (char *) env->GetStringUTFChars((jstring) element, JNI_FALSE);
            printf("%s\t", str);
        }
        printf("%d\t", carbonRow.getShort(5));
        printf("%d\t", carbonRow.getInt(6));
        printf("%ld\t", carbonRow.getLong(7));
        printf("%lf\t", carbonRow.getDouble(8));
        bool bool1 = carbonRow.getBoolean(9);
        if (bool1) {
            printf("true\t");
        } else {
            printf("false\t");
        }
        printf("%s\t", carbonRow.getDecimal(10));
        printf("%f\t", carbonRow.getFloat(11));
        printf("\n");
    }

    carbonReaderClass.close();
}

/**
 * test read data from local disk
 *
 * @param env  jni env
 * @return
 */
bool readFromLocal(JNIEnv *env) {

    CarbonReader reader;
    reader.builder(env, "../resources/carbondata", "test");

    char *argv[12];
    argv[0] = "stringField";
    argv[1] = "shortField";
    argv[2] = "intField";
    argv[3] = "longField";
    argv[4] = "doubleField";
    argv[5] = "boolField";
    argv[6] = "dateField";
    argv[7] = "timeField";
    argv[8] = "decimalField";
    argv[9] = "varcharField";
    argv[10] = "arrayField";
    argv[11] = "floatField";
    reader.projection(12, argv);

    reader.build();

    printf("\nRead data from local:\n");

    CarbonRow carbonRow(env);
    while (reader.hasNext()) {
        jobject row = reader.readNextCarbonRow();
        carbonRow.setCarbonRow(row);

        printf("%s\t", carbonRow.getString(0));
        printf("%d\t", carbonRow.getShort(1));
        printf("%d\t", carbonRow.getInt(2));
        printf("%ld\t", carbonRow.getLong(3));
        printf("%lf\t", carbonRow.getDouble(4));
        bool bool1 = carbonRow.getBoolean(5);
        if (bool1) {
            printf("true\t");
        } else {
            printf("false\t");
        }
        printf("%d\t", carbonRow.getInt(6));
        printf("%ld\t", carbonRow.getLong(7));
        printf("%s\t", carbonRow.getDecimal(8));
        printf("%s\t", carbonRow.getVarchar(9));
        jobjectArray jobjectArray1 = carbonRow.getArray(10);
        jsize length = env->GetArrayLength(jobjectArray1);
        int j = 0;
        for (j = 0; j < length; j++) {
            jobject element = env->GetObjectArrayElement(jobjectArray1, j);
            char *str = (char *) env->GetStringUTFChars((jstring) element, JNI_FALSE);
            printf("%s\t", str);
        }
        printf("%f\t", carbonRow.getFloat(11));
        printf("\n");
    }

    reader.close();
}

/**
 * read data from S3
 * parameter is ak sk endpoint
 *
 * @param env jni env
 * @param argv argument vector
 * @return
 */
bool readFromS3(JNIEnv *env, char *argv[]) {
    CarbonReader reader;

    char *args[3];
    // "your access key"
    args[0] = argv[1];
    // "your secret key"
    args[1] = argv[2];
    // "your endPoint"
    args[2] = argv[3];

    reader.builder(env, "s3a://sdk/WriterOutput/carbondata/", "test");
    reader.withHadoopConf("fs.s3a.access.key", argv[1]);
    reader.withHadoopConf("fs.s3a.secret.key", argv[2]);
    reader.withHadoopConf("fs.s3a.endpoint", argv[3]);
    reader.build();
    printf("\nRead data from S3:\n");
    CarbonRow carbonRow(env);
    while (reader.hasNext()) {
        jobject row = reader.readNextCarbonRow();
        carbonRow.setCarbonRow(row);

        printf("%s\t", carbonRow.getString(0));
        printf("%d\t", carbonRow.getInt(1));
        printf("%ld\t", carbonRow.getLong(2));
        printf("%s\t", carbonRow.getVarchar(3));
        jobjectArray jobjectArray1 = carbonRow.getArray(4);
        jsize length = env->GetArrayLength(jobjectArray1);
        int j = 0;
        for (j = 0; j < length; j++) {
            jobject element = env->GetObjectArrayElement(jobjectArray1, j);
            char *str = (char *) env->GetStringUTFChars((jstring) element, JNI_FALSE);
            printf("%s\t", str);
        }
        printf("%d\t", carbonRow.getShort(5));
        printf("%d\t", carbonRow.getInt(6));
        printf("%ld\t", carbonRow.getLong(7));
        printf("%lf\t", carbonRow.getDouble(8));
        bool bool1 = carbonRow.getBoolean(9);
        if (bool1) {
            printf("true\t");
        } else {
            printf("false\t");
        }
        printf("%s\t", carbonRow.getDecimal(10));
        printf("%f\t", carbonRow.getFloat(11));
        printf("\n");
    }

    reader.close();
}

/**
 * This a example for C++ interface to read carbon file
 * If you want to test read data fromS3, please input the parameter: ak sk endpoint
 *
 * @param argc argument counter
 * @param argv argument vector
 * @return
 */
int main(int argc, char *argv[]) {
    // init jvm
    JNIEnv *env;
    env = initJVM();

    if (argc > 3) {
        readFromS3(env, argv);
    } else {
        readFromLocalWithoutProjection(env);
        readFromLocal(env);
    }
    cout << "destory jvm\n\n";
    (jvm)->DestroyJavaVM();

    cout << "\nfinish destory jvm";
    fprintf(stdout, "Java VM destory.\n");
    return 0;
}

