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

using namespace std;

JavaVM *jvm;

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

bool readFromLocal(JNIEnv *env) {

    CarbonReader carbonReaderClass;
    carbonReaderClass.builder(env, "../resources/carbondata", "test");
    carbonReaderClass.build();

    printf("\nRead data from local:\n");

    while (carbonReaderClass.hasNext()) {
        jobjectArray row = carbonReaderClass.readNextRow();
        jsize length = env->GetArrayLength(row);

        int j = 0;
        for (j = 0; j < length; j++) {
            jobject element = env->GetObjectArrayElement(row, j);
            char *str = (char *) env->GetStringUTFChars((jstring) element, JNI_FALSE);
            printf("%s\t", str);
        }
        printf("\n");
    }

    carbonReaderClass.close();
}

bool readFromS3(JNIEnv *env) {
    CarbonReader carbonReaderClass;

    char *args[3];
    args[0] = "your access key";
    args[1] = "your secret key";
    args[2] = "obs.cn-north-1.myhwclouds.com";


    carbonReaderClass.builder(env, "s3a://sdk/WriterOutput/xubo", "test");
    carbonReaderClass.build(args[0], args[1], args[2]);

    printf("\nRead data from S3:\n");
    while (carbonReaderClass.hasNext()) {
        jobjectArray row = carbonReaderClass.readNextRow();
        jsize length = env->GetArrayLength(row);

        int j = 0;
        for (j = 0; j < length; j++) {
            jobject element = env->GetObjectArrayElement(row, j);
            char *str = (char *) env->GetStringUTFChars((jstring) element, JNI_FALSE);
            printf("%s\t", str);
        }
        printf("\n");
    }

    carbonReaderClass.close();
}

int main() {
    // init jvm
    JNIEnv *env;
    env = initJVM();

    readFromLocal(env);

//    readFromS3(env);
    cout << "destory jvm\n\n";
    (jvm)->DestroyJavaVM();

    cout << "\nfinish destory jvm";
    fprintf(stdout, "Java VM destory.\n");
    return 0;
}

