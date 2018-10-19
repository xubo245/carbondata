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

#include "CarbonWriter.h"

jobject CarbonWriter::builder(JNIEnv *env) {
    jniEnv = env;
    carbonWriter = env->FindClass("org/apache/carbondata/sdk/file/CarbonWriter");
    jmethodID carbonWriterBuilderID = env->GetStaticMethodID(carbonWriter, "builder",
        "()Lorg/apache/carbondata/sdk/file/CarbonWriterBuilder;");
    carbonWriterBuilderObject = env->CallStaticObjectMethod(carbonWriter, carbonWriterBuilderID);
    return carbonWriterBuilderObject;
}

jobject CarbonWriter::outputPath(char *path) {
    jclass carbonWriterBuilderClass = jniEnv->GetObjectClass(carbonWriterBuilderObject);
    jmethodID carbonWriterBuilderID = jniEnv->GetMethodID(carbonWriterBuilderClass, "outputPath",
        "(Ljava/lang/String;)Lorg/apache/carbondata/sdk/file/CarbonWriterBuilder;");
    jstring jPath = jniEnv->NewStringUTF(path);
    jvalue args[1];
    args[0].l = jPath;
    carbonWriterBuilderObject = jniEnv->CallObjectMethodA(carbonWriterBuilderObject, carbonWriterBuilderID, args);
    return carbonWriterBuilderObject;
}

jobject CarbonWriter::withCsvInput(char *jsonSchema) {

    jclass carbonWriterBuilderClass = jniEnv->GetObjectClass(carbonWriterBuilderObject);
    jmethodID carbonWriterBuilderID = jniEnv->GetMethodID(carbonWriterBuilderClass, "withCsvInput",
        "(Ljava/lang/String;)Lorg/apache/carbondata/sdk/file/CarbonWriterBuilder;");
    jstring jPath = jniEnv->NewStringUTF(jsonSchema);
    jvalue args[1];
    args[0].l = jPath;
    carbonWriterBuilderObject = jniEnv->CallObjectMethodA(carbonWriterBuilderObject, carbonWriterBuilderID, args);
    return carbonWriterBuilderObject;
};

jobject CarbonWriter::withHadoopConf(char *key, char *value) {
    jclass carbonWriterBuilderClass = jniEnv->GetObjectClass(carbonWriterBuilderObject);
    jmethodID buildID = jniEnv->GetMethodID(carbonWriterBuilderClass, "withHadoopConf",
        "(Ljava/lang/String;Ljava/lang/String;)Lorg/apache/carbondata/sdk/file/CarbonWriterBuilder;");
    jvalue args[2];
    args[0].l = jniEnv->NewStringUTF(key);
    args[1].l = jniEnv->NewStringUTF(value);
    carbonWriterBuilderObject = jniEnv->CallObjectMethodA(carbonWriterBuilderObject, buildID, args);
    return carbonWriterBuilderObject;
}

jobject CarbonWriter::build() {
    jclass carbonWriterBuilderClass = jniEnv->GetObjectClass(carbonWriterBuilderObject);
    jmethodID buildID = jniEnv->GetMethodID(carbonWriterBuilderClass, "build",
        "()Lorg/apache/carbondata/sdk/file/CarbonWriter;");
    carbonWriterObject = jniEnv->CallObjectMethod(carbonWriterBuilderObject, buildID);
    carbonWriter = jniEnv->GetObjectClass(carbonWriterObject);
    writeID = jniEnv->GetMethodID(carbonWriter, "write", "(Ljava/lang/Object;)V");
    return carbonWriterObject;
}

void CarbonWriter::write(jobject obj) {
    jvalue args[1];
    args[0].l = obj;
    jniEnv->CallBooleanMethodA(carbonWriterObject, writeID, args);
};

jboolean CarbonWriter::close() {
    jclass carbonWriter = jniEnv->GetObjectClass(carbonWriterObject);
    jmethodID closeID = jniEnv->GetMethodID(carbonWriter, "close", "()V");
    jniEnv->CallBooleanMethod(carbonWriterObject, closeID);
}