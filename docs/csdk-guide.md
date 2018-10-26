<!--
    Licensed to the Apache Software Foundation (ASF) under one or more 
    contributor license agreements.  See the NOTICE file distributed with
    this work for additional information regarding copyright ownership. 
    The ASF licenses this file to you under the Apache License, Version 2.0
    (the "License"); you may not use this file except in compliance with 
    the License.  You may obtain a copy of the License at

      http://www.apache.org/licenses/LICENSE-2.0
    
    Unless required by applicable law or agreed to in writing, software 
    distributed under the License is distributed on an "AS IS" BASIS, 
    WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
    See the License for the specific language governing permissions and 
    limitations under the License.
-->

# CSDK Guide

CarbonData CSDK provides C++ interface to write and read carbon file. 
CSDK use JNI to invoke java SDK in C++ code.


# CSDK Reader
This CSDK reader reads CarbonData file and carbonindex file at a given path.
External client can make use of this reader to read CarbonData files in C++ 
code and without CarbonSession.


In the carbon jars package, there exist a carbondata-sdk.jar, 
including SDK reader for CSDK.
## Quick example

Please find example code at  [main.cpp](https://github.com/apache/carbondata/blob/master/store/CSDK/test/main.cpp) of CSDK module  

When users use C++ to read carbon files, users should init JVM first. Then users create 
carbon reader and read data.There are some example code of read data from local disk  
and read data from S3 at main.cpp of CSDK module.  Finally, Finally, users need to 
release the memory and destroy JVM.

## API List
### read data
```
    /**
     * create a CarbonReaderBuilder object for building carbonReader,
     * CarbonReaderBuilder object  can configure different parameter
     *
     * @param env JNIEnv
     * @param path data store path
     * @param tableName table name
     * @return CarbonReaderBuilder object
     */
    jobject builder(JNIEnv *env, char *path, char *tableName);

    /**
     * Configure the projection column names of carbon reader
     *
     * @param argc argument counter
     * @param argv argument vector
     * @return CarbonReaderBuilder object
     */
    jobject projection(int argc, char *argv[]);

    /**
     *  build carbon reader with argument vector
     *  it support multiple parameter
     *  like: key=value
     *  for example: fs.s3a.access.key=XXXX, XXXX is user's access key value
     *
     * @param argc argument counter
     * @param argv argument vector
     * @return CarbonReaderBuilder object
     **/
    jobject withHadoopConf(int argc, char *argv[]);

    /**
     * build carbonReader object for reading data
     * it support read data from load disk
     *
     * @return carbonReader object
     */
    jobject build();

    /**
     * Whether it has next row data
     *
     * @return boolean value, if it has next row, return true. if it hasn't next row, return false.
     */
    jboolean hasNext();

    /**
     * read next carbonRow from data
     * @return carbonRow object of one row
     */
     jobject readNextRow();

    /**
     * close the carbon reader
     *
     * @return  boolean value
     */
    jboolean close();

```

### CarbonSchemaReader

```
    /**
     * constructor with jni env
     *
     * @param env  jni env
     */
    CarbonSchemaReader(JNIEnv *env);
```
```
    /**
     * read Schema from Data File
     *
     * @param path Data File path
     * @return carbon schema object
     */
    jobject readSchemaInDataFile(char *path);
```
```
    /**
     * read Schema from index File
     *
     * @param path index File path
     * @return carbon schema object
     */
    jobject readSchemaInIndexFile(char *path);

```
###Schema
``` 
 /**
     * constructor with jni env and carbon schema data
     *
     * @param env jni env
     * @param schema  carbon schema data
     */
    Schema(JNIEnv *env, jobject schema);

```
```
    /**
     * get fields length of schema
     *
     * @return fields length
     */
    int getFieldsLength();

```
```
    /**
     * get field name by ordinal
     *
     * @param ordinal the data index of carbon schema
     * @return ordinal field name
     */
    char *getFieldName(int ordinal);

```
```
    /**
     * get  field data type name by ordinal
     *
     * @param ordinal the data index of carbon schema
     * @return ordinal field data type name
     */
    char *getFieldDataTypeName(int ordinal);

```
```
    /**
     * get  array child element data type name by ordinal
     *
     * @param ordinal the data index of carbon schema
     * @return ordinal array child element data type name
     */
    char *getArrayElementTypeName(int ordinal);
```