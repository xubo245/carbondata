#include <jni.h>
#include "Configuration.h"

Configuration::Configuration(JNIEnv *env) {
    jclass configurationClass = env->FindClass("org/apache/hadoop/conf/Configuration");
    jmethodID con = env->GetMethodID(configurationClass, "<init>", "()V");
    configurationObject = env->NewObject(configurationClass, con);
    jniEnv = env;
}

jobject Configuration::set(char *key, char *value) {
    jclass configurationClass = jniEnv->GetObjectClass(configurationObject);
    jmethodID setID = jniEnv->GetMethodID(configurationClass, "set", "(Ljava/lang/String;Ljava/lang/String;)V");
    jstring keyJ = jniEnv->NewStringUTF(key);
    jstring valueJ = jniEnv->NewStringUTF(value);
    jvalue args[2];
    args[0].l = keyJ;
    args[1].l = valueJ;
    jniEnv->CallObjectMethodA(configurationObject, setID, args);


    jclass configurationClass2 = jniEnv->GetObjectClass(configurationObject);
    jmethodID getID = jniEnv->GetMethodID(configurationClass2, "get", "(Ljava/lang/String;)Ljava/lang/String;");
    jvalue getArgs[1];
    args[0].l = keyJ;

    jstring  result = (jstring) jniEnv->CallObjectMethodA(configurationObject, getID, getArgs);

    char *str = (char *) jniEnv->GetStringUTFChars((jstring) result, JNI_FALSE);
    printf("value:%s\t", str);

    return configurationObject;
}