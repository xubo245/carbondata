//
// Created by Xubo on 21/09/2018.
//

#include <jni.h>

#ifndef CJDK_CONFIGURATION_H
#define CJDK_CONFIGURATION_H

#endif //CJDK_CONFIGURATION_H

class Configuration {
public:
    Configuration(JNIEnv *env);

    jobject configurationObject;
    JNIEnv *jniEnv;

    jobject set(char *key, char *value);
};