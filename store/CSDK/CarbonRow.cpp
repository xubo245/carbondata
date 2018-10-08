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

CarbonRow::CarbonRow(JNIEnv *env, jobject jobject1) {
    this->jniEnv = env;
    this->carbonRow = jobject1;
}

void CarbonRow::print(int ordinal) {
    char *dataTypeName = getDataTypeName(ordinal);
    if (strcmp(dataTypeName, "INT") == 0
        || strcmp(dataTypeName, "DATE") == 0) {
        printf("%d\t", getInt(ordinal));
    } else if (strcmp(dataTypeName, "LONG") == 0
               || strcmp(dataTypeName, "TIMESTAMP") == 0) {
        printf("%ld\t", getLong(ordinal));
    } else if (strcmp(dataTypeName, "STRING") == 0) {
        printf("%s\t", getString(ordinal));
    } else if (strcmp(dataTypeName, "DOUBLE") == 0) {
        printf("%lf\t", getDouble(ordinal));
    } else if (strcmp(dataTypeName, "SHORT") == 0) {
        printf("%d\t", getShort(ordinal));
    } else if (strcmp(dataTypeName, "BOOLEAN") == 0) {
        bool bool1 = getBoolean(ordinal);
        if (bool1) {
            printf("true\t");
        } else {
            printf("false\t");
        }
    } else if (strcmp(dataTypeName, "VARCHAR") == 0) {
        printf("%s\t", getVarchar(ordinal));
    } else if (strcmp(dataTypeName, "DECIMAL") == 0) {
        printf("%s\t", getDecimal(ordinal));
    } else if (strcmp(dataTypeName, "FLOAT") == 0) {
        printf("%f\t", getFloat(ordinal));
    } else if (strcmp(dataTypeName, "ARRAY") == 0) {
        jobjectArray jobjectArray1 = getArray(ordinal);
        jsize length = jniEnv->GetArrayLength(jobjectArray1);
        int j = 0;
        for (j = 0; j < length; j++) {
            if (strcmp(getArrayElementTypeName(ordinal), "STRING") == 0) {
                jobject element = jniEnv->GetObjectArrayElement(jobjectArray1, j);
                char *str = (char *) jniEnv->GetStringUTFChars((jstring) element, JNI_FALSE);
                printf("%s\t", str);
            }
        }
    } else {
        printf("%s\t", getString(ordinal));
    }
}

int CarbonRow::getLength() {
    jclass carbonReaderBuilderClass = jniEnv->GetObjectClass(carbonRow);
    jmethodID buildID = jniEnv->GetMethodID(carbonReaderBuilderClass, "getLength",
                                            "()I");
    return jniEnv->CallIntMethod(carbonRow, buildID);
}

char *CarbonRow::getDataTypeName(int ordinal) {
    jclass carbonReaderBuilderClass = jniEnv->GetObjectClass(carbonRow);
    jmethodID buildID = jniEnv->GetMethodID(carbonReaderBuilderClass, "getDataTypeName",
                                            "(I)Ljava/lang/String;");
    jvalue args[1];
    args[0].i = ordinal;
    jobject data = jniEnv->CallObjectMethodA(carbonRow, buildID, args);

    char *str = (char *) jniEnv->GetStringUTFChars((jstring) data, JNI_FALSE);
    return str;
}

char *CarbonRow::getArrayElementTypeName(int ordinal) {
    jclass carbonReaderBuilderClass = jniEnv->GetObjectClass(carbonRow);
    jmethodID buildID = jniEnv->GetMethodID(carbonReaderBuilderClass, "getArrayElementTypeName",
                                            "(I)Ljava/lang/String;");
    jvalue args[1];
    args[0].i = ordinal;
    jobject data = jniEnv->CallObjectMethodA(carbonRow, buildID, args);

    char *str = (char *) jniEnv->GetStringUTFChars((jstring) data, JNI_FALSE);
    return str;
}

short CarbonRow::getShort(int ordinal) {
    jclass carbonReaderBuilderClass = jniEnv->GetObjectClass(carbonRow);
    jmethodID buildID = jniEnv->GetMethodID(carbonReaderBuilderClass, "getShort",
                                            "(I)S");
    jvalue args[1];
    args[0].i = ordinal;
    return jniEnv->CallIntMethodA(carbonRow, buildID, args);
}

int CarbonRow::getInt(int ordinal) {
    jclass carbonReaderBuilderClass = jniEnv->GetObjectClass(carbonRow);
    jmethodID buildID = jniEnv->GetMethodID(carbonReaderBuilderClass, "getInt",
                                            "(I)I");
    jvalue args[1];
    args[0].i = ordinal;
    return jniEnv->CallIntMethodA(carbonRow, buildID, args);
}

long CarbonRow::getLong(int ordinal) {
    jclass carbonReaderBuilderClass = jniEnv->GetObjectClass(carbonRow);
    jmethodID buildID = jniEnv->GetMethodID(carbonReaderBuilderClass, "getLong",
                                            "(I)J");
    jvalue args[1];
    args[0].i = ordinal;
    return jniEnv->CallLongMethodA(carbonRow, buildID, args);
}

double CarbonRow::getDouble(int ordinal) {
    jclass carbonReaderBuilderClass = jniEnv->GetObjectClass(carbonRow);
    jmethodID buildID = jniEnv->GetMethodID(carbonReaderBuilderClass, "getDouble",
                                            "(I)D");
    jvalue args[1];
    args[0].i = ordinal;
    return jniEnv->CallDoubleMethodA(carbonRow, buildID, args);
}


float CarbonRow::getFloat(int ordinal) {
    jclass carbonReaderBuilderClass = jniEnv->GetObjectClass(carbonRow);
    jmethodID buildID = jniEnv->GetMethodID(carbonReaderBuilderClass, "getFloat",
                                            "(I)F");
    jvalue args[1];
    args[0].i = ordinal;
    return jniEnv->CallFloatMethodA(carbonRow, buildID, args);
}

jboolean CarbonRow::getBoolean(int ordinal) {
    jclass carbonReaderBuilderClass = jniEnv->GetObjectClass(carbonRow);
    jmethodID buildID = jniEnv->GetMethodID(carbonReaderBuilderClass, "getBoolean",
                                            "(I)Z");
    jvalue args[1];
    args[0].i = ordinal;
    return jniEnv->CallBooleanMethodA(carbonRow, buildID, args);
}

char *CarbonRow::getString(int ordinal) {
    jclass carbonReaderBuilderClass = jniEnv->GetObjectClass(carbonRow);
    jmethodID buildID = jniEnv->GetMethodID(carbonReaderBuilderClass, "getString",
                                            "(I)Ljava/lang/String;");
    jvalue args[1];
    args[0].i = ordinal;
    jobject data = jniEnv->CallObjectMethodA(carbonRow, buildID, args);

    char *str = (char *) jniEnv->GetStringUTFChars((jstring) data, JNI_FALSE);
    return str;
}

char *CarbonRow::getDecimal(int ordinal) {
    jclass carbonReaderBuilderClass = jniEnv->GetObjectClass(carbonRow);
    jmethodID buildID = jniEnv->GetMethodID(carbonReaderBuilderClass, "getDecimal",
                                            "(I)Ljava/lang/String;");
    jvalue args[1];
    args[0].i = ordinal;
    jobject data = jniEnv->CallObjectMethodA(carbonRow, buildID, args);

    char *str = (char *) jniEnv->GetStringUTFChars((jstring) data, JNI_FALSE);
    return str;
}

char *CarbonRow::getVarchar(int ordinal) {
    jclass carbonReaderBuilderClass = jniEnv->GetObjectClass(carbonRow);
    jmethodID buildID = jniEnv->GetMethodID(carbonReaderBuilderClass, "getVarchar",
                                            "(I)Ljava/lang/String;");
    jvalue args[1];
    args[0].i = ordinal;
    jobject data = jniEnv->CallObjectMethodA(carbonRow, buildID, args);

    char *str = (char *) jniEnv->GetStringUTFChars((jstring) data, JNI_FALSE);
    return str;
}

jobjectArray CarbonRow::getArray(int ordinal) {
    jclass carbonReaderBuilderClass = jniEnv->GetObjectClass(carbonRow);
    jmethodID buildID = jniEnv->GetMethodID(carbonReaderBuilderClass, "getArray",
                                            "(I)[Ljava/lang/Object;");
    jvalue args[1];
    args[0].i = ordinal;
    jobjectArray data = (jobjectArray) jniEnv->CallObjectMethodA(carbonRow, buildID, args);
    return data;
}