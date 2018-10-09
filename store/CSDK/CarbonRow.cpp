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
#include <cstring>
#include "CarbonRow.h"

CarbonRow::CarbonRow(JNIEnv *env) {
    this->rowUtilClass = env->FindClass("org/apache/carbondata/sdk/file/RowUtil");
    this->jniEnv = env;
}

void CarbonRow::setCarbonRow(jobject data) {
    this->carbonRow = data;
}

short CarbonRow::getShort(int ordinal) {
    jmethodID buildID = jniEnv->GetStaticMethodID(rowUtilClass, "getShort",
        "([Ljava/lang/Object;I)S");
    jvalue args[2];
    args[0].l = carbonRow;
    args[1].i = ordinal;
    return jniEnv->CallStaticShortMethodA(rowUtilClass, buildID, args);
}

int CarbonRow::getInt(int ordinal) {
    jmethodID buildID = jniEnv->GetStaticMethodID(rowUtilClass, "getInt",
        "([Ljava/lang/Object;I)I");
    jvalue args[2];
    args[0].l = carbonRow;
    args[1].i = ordinal;
    return jniEnv->CallStaticIntMethodA(rowUtilClass, buildID, args);
}

long CarbonRow::getLong(int ordinal) {
    jmethodID buildID = jniEnv->GetStaticMethodID(rowUtilClass, "getLong",
        "([Ljava/lang/Object;I)J");
    jvalue args[2];
    args[0].l = carbonRow;
    args[1].i = ordinal;
    return jniEnv->CallStaticLongMethodA(rowUtilClass, buildID, args);
}

double CarbonRow::getDouble(int ordinal) {
    jmethodID buildID = jniEnv->GetStaticMethodID(rowUtilClass, "getDouble",
        "([Ljava/lang/Object;I)D");
    jvalue args[2];
    args[0].l = carbonRow;
    args[1].i = ordinal;
    return jniEnv->CallStaticDoubleMethodA(rowUtilClass, buildID, args);
}


float CarbonRow::getFloat(int ordinal) {
    jmethodID buildID = jniEnv->GetStaticMethodID(rowUtilClass, "getFloat",
        "([Ljava/lang/Object;I)F");
    jvalue args[2];
    args[0].l = carbonRow;
    args[1].i = ordinal;
    return jniEnv->CallStaticFloatMethodA(rowUtilClass, buildID, args);
}

jboolean CarbonRow::getBoolean(int ordinal) {
    jmethodID buildID = jniEnv->GetStaticMethodID(rowUtilClass, "getBoolean",
        "([Ljava/lang/Object;I)Z");
    jvalue args[2];
    args[0].l = carbonRow;
    args[1].i = ordinal;
    return jniEnv->CallStaticBooleanMethodA(rowUtilClass, buildID, args);
}

char *CarbonRow::getString(int ordinal) {
    jmethodID buildID = jniEnv->GetStaticMethodID(rowUtilClass, "getString",
        "([Ljava/lang/Object;I)Ljava/lang/String;");
    jvalue args[2];
    args[0].l = carbonRow;
    args[1].i = ordinal;
    jobject data = jniEnv->CallStaticObjectMethodA(rowUtilClass, buildID, args);

    char *str = (char *) jniEnv->GetStringUTFChars((jstring) data, JNI_FALSE);
    return str;
}

char *CarbonRow::getDecimal(int ordinal) {
    jmethodID buildID = jniEnv->GetStaticMethodID(rowUtilClass, "getDecimal",
         "([Ljava/lang/Object;I)Ljava/lang/String;");
    jvalue args[2];
    args[0].l = carbonRow;
    args[1].i = ordinal;
    jobject data = jniEnv->CallStaticObjectMethodA(rowUtilClass, buildID, args);

    char *str = (char *) jniEnv->GetStringUTFChars((jstring) data, JNI_FALSE);
    return str;
}

char *CarbonRow::getVarchar(int ordinal) {
    jmethodID buildID = jniEnv->GetStaticMethodID(rowUtilClass, "getVarchar",
       "([Ljava/lang/Object;I)Ljava/lang/String;");
    jvalue args[2];
    args[0].l = carbonRow;
    args[1].i = ordinal;
    jobject data = jniEnv->CallStaticObjectMethodA(rowUtilClass, buildID, args);
    char *str = (char *) jniEnv->GetStringUTFChars((jstring) data, JNI_FALSE);
    return str;
}

jobjectArray CarbonRow::getArray(int ordinal) {
    jmethodID buildID = jniEnv->GetStaticMethodID(rowUtilClass, "getArray",
        "([Ljava/lang/Object;I)[Ljava/lang/Object;");
    jvalue args[2];
    args[0].l = carbonRow;
    args[1].i = ordinal;
    return (jobjectArray) jniEnv->CallStaticObjectMethodA(rowUtilClass, buildID, args);
}