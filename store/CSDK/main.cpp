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
#include <sys/time.h>

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
    int parNum = 2;
    int res;
    JavaVMOption options[parNum];

    options[0].optionString = "-Djava.class.path=../../sdk/target/carbondata-sdk.jar";
    options[1].optionString = "-verbose:jni";                // For debug and check the jni information
    //    options[2].optionString = "-Xms5000m";             // change the jvm min memory size
    //    options[3].optionString = "-Xmx16000m";            // change the jvm max memory size
    //    options[4].optionString = "-Djava.compiler=NONE";  // forbidden JIT
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
bool readFromLocalWithoutProjection(JNIEnv *env, char *path) {

    CarbonReader carbonReaderClass;
    carbonReaderClass.builder(env, path);
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
        jobjectArray array1 = carbonRow.getArray(4);
        jsize length = env->GetArrayLength(array1);
        int j = 0;
        for (j = 0; j < length; j++) {
            jobject element = env->GetObjectArrayElement(array1, j);
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
        env->DeleteLocalRef(row);
        env->DeleteLocalRef(array1);
    }

    carbonReaderClass.close();
}

/**
 * test next Row Performance
 *
 * @param env  jni env
 * @return
 */
bool testNextRowPerformance(JNIEnv *env, char *path, int printNum, char *argv[], int argc) {
    printf("\nTest next Row Performance:\n");

    struct timeval start, build, startRead, endBatchRead, endRead;
    gettimeofday(&start, NULL);

    CarbonReader carbonReaderClass;

    carbonReaderClass.builder(env, path);
    if (argc > 1) {
        char *args[3];
        // "your access key"
        args[0] = argv[1];
        // "your secret key"
        args[1] = argv[2];
        // "your endPoint"
        args[2] = argv[3];


        carbonReaderClass.withHadoopConf("fs.s3a.access.key", argv[1]);
        carbonReaderClass.withHadoopConf("fs.s3a.secret.key", argv[2]);
        carbonReaderClass.withHadoopConf("fs.s3a.endpoint", argv[3]);
    }
    carbonReaderClass.build();

    gettimeofday(&build, NULL);
    int time = 1000000 * (build.tv_sec - start.tv_sec) + build.tv_usec - start.tv_usec;
    double buildTime = time / 1000000.0;
    printf("\n\nbuild time is: %lf s\n\n", time / 1000000.0);

    CarbonRow carbonRow(env);
    int i = 0;

    gettimeofday(&startRead, NULL);
    jobject row;
    while (carbonReaderClass.hasNext()) {

        row = carbonReaderClass.readNextCarbonRow();

        i++;
        if (i > 1 && i % printNum == 0) {
            gettimeofday(&endBatchRead, NULL);

            time = 1000000 * (endBatchRead.tv_sec - startRead.tv_sec) + endBatchRead.tv_usec - startRead.tv_usec;
            printf("%d: time is %lf s, speed is %lf records/s  ", i, time / 1000000.0, printNum / (time / 1000000.0));

            carbonRow.setCarbonRow(row);
            printf("%s\t", carbonRow.getString(0));
            printf("%s\t", carbonRow.getString(1));
            printf("%s\t", carbonRow.getString(2));
            printf("%s\t", carbonRow.getString(3));
            printf("%ld\t", carbonRow.getLong(4));
            printf("%ld\t", carbonRow.getLong(5));
            printf("\n");

            gettimeofday(&startRead, NULL);
        }
        env->DeleteLocalRef(row);
    }

    gettimeofday(&endRead, NULL);

    time = 1000000 * (endRead.tv_sec - build.tv_sec) + endRead.tv_usec - build.tv_usec;
    printf("total line is: %d,\t build time is: %lf s,\tread time is %lf s, average speed is %lf records/s  ",
           i, buildTime, time / 1000000.0, i / (time / 1000000.0));
    carbonReaderClass.close();
}

/**
 * test Batch Row Performance
 *
 * @param env  jni env
 * @return
 */
bool testNextBatchRowPerformance(JNIEnv *env, char *path, int batchSize, int printNum, char *argv[], int argc) {
    printf("\n\nTest next Batch Row Performance:\n");

    struct timeval start, build, read;
    gettimeofday(&start, NULL);

    CarbonReader carbonReaderClass;

    carbonReaderClass.builder(env, path);
    if (argc > 1) {
        char *args[3];
        // "your access key"
        args[0] = argv[1];
        // "your secret key"
        args[1] = argv[2];
        // "your endPoint"
        args[2] = argv[3];

        carbonReaderClass.withHadoopConf("fs.s3a.access.key", argv[1]);
        carbonReaderClass.withHadoopConf("fs.s3a.secret.key", argv[2]);
        carbonReaderClass.withHadoopConf("fs.s3a.endpoint", argv[3]);
    }
    carbonReaderClass.withBatch(batchSize);
    carbonReaderClass.build();

    gettimeofday(&build, NULL);
    int time = 1000000 * (build.tv_sec - start.tv_sec) + build.tv_usec - start.tv_usec;
    double buildTime = time / 1000000.0;
    printf("\n\nbuild time is: %lf s\n\n", time / 1000000.0);

    CarbonRow carbonRow(env);
    int i = 0;
    struct timeval startHasNext, startReadNextBatchRow, endReadNextBatchRow, endRead;
    gettimeofday(&startHasNext, NULL);

    while (carbonReaderClass.hasNext()) {

        gettimeofday(&startReadNextBatchRow, NULL);
        jobjectArray batch = carbonReaderClass.readNextBatchRow(batchSize);
        gettimeofday(&endReadNextBatchRow, NULL);

        jsize length = env->GetArrayLength(batch);
        if (i + length > printNum - 1) {
            for (int j = 0; j < length; j++) {
                i++;
                jobject row = env->GetObjectArrayElement(batch, j);
                if (i > 1 && i % printNum == 0) {
                    gettimeofday(&read, NULL);

                    double hasNextTime = 1000000 * (startReadNextBatchRow.tv_sec - startHasNext.tv_sec) +
                                         startReadNextBatchRow.tv_usec - startHasNext.tv_usec;

                    double readNextBatchTime = 1000000 * (endReadNextBatchRow.tv_sec - startReadNextBatchRow.tv_sec) +
                                               endReadNextBatchRow.tv_usec - startReadNextBatchRow.tv_usec;

                    time = 1000000 * (read.tv_sec - startHasNext.tv_sec) + read.tv_usec - startHasNext.tv_usec;
                    printf("%d: time is %lf s, speed is %lf records/s, hasNext time is %lf s,readNextBatchRow time is %lf s ",
                           i, time / 1000000.0, printNum / (time / 1000000.0), hasNextTime / 1000000.0,
                           readNextBatchTime / 1000000.0);

                    carbonRow.setCarbonRow(row);
                    printf("%s\t", carbonRow.getString(0));
                    printf("%s\t", carbonRow.getString(1));
                    printf("%s\t", carbonRow.getString(2));
                    printf("%s\t", carbonRow.getString(3));
                    printf("%ld\t", carbonRow.getLong(4));
                    printf("%ld\t", carbonRow.getLong(5));
                    printf("\n");
                }
                env->DeleteLocalRef(row);
            }
        } else {
            i = i + length;
        }
        env->DeleteLocalRef(batch);
        gettimeofday(&startHasNext, NULL);
    }
    gettimeofday(&endRead, NULL);
    time = 1000000 * (endRead.tv_sec - build.tv_sec) + endRead.tv_usec - build.tv_usec;
    printf("total line is: %d,\t build time is: %lf s,\tread time is %lf s, average speed is %lf records/s  ",
           i, buildTime, time / 1000000.0, i / (time / 1000000.0));
    carbonReaderClass.close();
}

/**
 * test read data from local disk
 *
 * @param env  jni env
 * @return
 */
bool readFromLocal(JNIEnv *env, char *path) {

    CarbonReader reader;
    reader.builder(env, path, "test");

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
        jobjectArray array1 = carbonRow.getArray(10);
        jsize length = env->GetArrayLength(array1);
        int j = 0;
        for (j = 0; j < length; j++) {
            jobject element = env->GetObjectArrayElement(array1, j);
            char *str = (char *) env->GetStringUTFChars((jstring) element, JNI_FALSE);
            printf("%s\t", str);
        }
        printf("%f\t", carbonRow.getFloat(11));
        printf("\n");
        env->DeleteLocalRef(row);
        env->DeleteLocalRef(array1);
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
    printf("\nRead data from S3:\n");
    struct timeval start, build, read;
    gettimeofday(&start, NULL);

    CarbonReader reader;

    char *args[3];
    // "your access key"
    args[0] = argv[1];
    // "your secret key"
    args[1] = argv[2];
    // "your endPoint"
    args[2] = argv[3];

    reader.builder(env, "s3a://sdk/WriterOutput/carbondata", "test");
    reader.withHadoopConf("fs.s3a.access.key", argv[1]);
    reader.withHadoopConf("fs.s3a.secret.key", argv[2]);
    reader.withHadoopConf("fs.s3a.endpoint", argv[3]);
    reader.build();

    gettimeofday(&build, NULL);
    int time = 1000000 * (build.tv_sec - start.tv_sec) + build.tv_usec - start.tv_usec;
    int buildTime = time / 1000000.0;
    printf("build time: %lf s\n", time / 1000000.0);

    CarbonRow carbonRow(env);
    int i = 0;
    while (reader.hasNext()) {
        jobject row = reader.readNextCarbonRow();
        i++;
        carbonRow.setCarbonRow(row);

        printf("%s\t", carbonRow.getString(0));
        printf("%d\t", carbonRow.getInt(1));
        printf("%ld\t", carbonRow.getLong(2));
        printf("%s\t", carbonRow.getVarchar(3));
        jobjectArray array1 = carbonRow.getArray(4);
        jsize length = env->GetArrayLength(array1);
        int j = 0;
        for (j = 0; j < length; j++) {
            jobject element = env->GetObjectArrayElement(array1, j);
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
        env->DeleteLocalRef(row);
        env->DeleteLocalRef(array1);
    }
    gettimeofday(&read, NULL);
    time = 1000000 * (read.tv_sec - start.tv_sec) + read.tv_usec - start.tv_usec;
    printf("total lines is %d: build time: %lf, read time is %lf s, average speed is %lf records/s\n",
           i, buildTime, time / 1000000.0, i / (time / 1000000.0));

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
bool readPerformanceFromS3(JNIEnv *env, char *argv[]) {
    printf("\nRead data from S3:\n");
    struct timeval start, build, startBatchRead, endBatchRead, endRead;
    gettimeofday(&start, NULL);

    CarbonReader reader;

    char *args[3];
    // "your access key"
    args[0] = argv[1];
    // "your secret key"
    args[1] = argv[2];
    // "your endPoint"
    args[2] = argv[3];

    reader.builder(env, "s3a://sdk/ges/oneFile", "test");
    reader.withHadoopConf("fs.s3a.access.key", argv[1]);
    reader.withHadoopConf("fs.s3a.secret.key", argv[2]);
    reader.withHadoopConf("fs.s3a.endpoint", argv[3]);
    reader.build();

    gettimeofday(&build, NULL);
    int time = 1000000 * (build.tv_sec - start.tv_sec) + build.tv_usec - start.tv_usec;
    printf("build time: %lf s\n", time / 1000000.0);
    int buildTime = time / 1000000.0;

    CarbonRow carbonRow(env);
    int i = 0;
    int printNum = 100000;
    gettimeofday(&startBatchRead, NULL);
    while (reader.hasNext()) {
        jobject row = reader.readNextCarbonRow();
        i++;
        if (i % printNum == 0) {
            gettimeofday(&endBatchRead, NULL);
            time = 1000000 * (endBatchRead.tv_sec - startBatchRead.tv_sec) + endBatchRead.tv_usec -
                   startBatchRead.tv_usec;
            printf("%d: time is %lf s, speed is %lf records/s.\t", i, time / 1000000.0, printNum / (time / 1000000.0));

            carbonRow.setCarbonRow(row);
            printf("%s\t", carbonRow.getString(0));
            printf("%s\t", carbonRow.getString(1));
            printf("%s\t", carbonRow.getString(2));
            printf("%s\t", carbonRow.getString(3));
            printf("%ld\t", carbonRow.getLong(4));
            printf("%ld\t", carbonRow.getLong(5));
            printf("\n");
            gettimeofday(&startBatchRead, NULL);
        }
        env->DeleteLocalRef(row);
    }
    gettimeofday(&endRead, NULL);

    time = 1000000 * (endRead.tv_sec - build.tv_sec) + endRead.tv_usec - build.tv_usec;
    printf("total lines is %d: build time: %lf, endBatchRead time is %lf s, average speed is %lf records/s\n",
           i, buildTime, time / 1000000.0, i / (time / 1000000.0));
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

    char *smallFilePath = "../../../../resources/carbondata";
    char *path = "../../../../../../../Downloads/carbon-data-big";
    char *S3Path = "s3a://sdk/ges/i400bs128";

    if (argc > 3) {
        cout << "Test performance on S3:\n";
        readFromS3(env, argv);
        readPerformanceFromS3(env, argv);
        testNextRowPerformance(env, S3Path, 100000, argv, 4);
        testNextBatchRowPerformance(env, S3Path, 100000, 100000, argv, 4);
    } else {
        readFromLocalWithoutProjection(env, smallFilePath);
        readFromLocal(env, smallFilePath);
        testNextRowPerformance(env, path, 100000, argv, 0);
        testNextBatchRowPerformance(env, path, 100000, 100000, argv, 0);
    }
    cout << "destory jvm\n\n";
    (jvm)->DestroyJavaVM();

    cout << "\nfinish destory jvm";
    fprintf(stdout, "Java VM destory.\n");
    return 0;
}

