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

package org.apache.carbondata.core.datastore.row;

import java.io.Serializable;
import java.math.BigDecimal;
import java.util.Arrays;

import org.apache.carbondata.core.cache.dictionary.Dictionary;
import org.apache.carbondata.core.metadata.datatype.ArrayType;
import org.apache.carbondata.core.metadata.datatype.DataType;


/**
 * This row class is used to transfer the row data from one step to other step
 */
public class Row implements Serializable {

  private Object[] data;

  private DataType[] dataTypes;

  public Row(Object[] data) {
    this.data = data;
  }

  /**
   * constructor
   * create with data and data type
   *
   * @param data      carbon row data
   * @param dataTypes data types
   * @param dictionaries Dictionary array
   */
  public Row(Object[] data, DataType[] dataTypes, Dictionary[] dictionaries) {
    assert (data.length == dictionaries.length);
    for (int i = 0; i < dictionaries.length; i++) {
      if (dictionaries[i] != null) {
        data[i] = dictionaries[i].getDictionaryValueForKey((int) data[i]);
      }
    }
    this.data = data;
    this.dataTypes = dataTypes;
  }

  public Object[] getData() {
    return data;
  }

  public void setData(Object[] data) {
    this.data = data;
  }

  public String getString(int ordinal) {
    return (String) data[ordinal];
  }

  /**
   * get short type data by ordinal
   *
   * @param ordinal the data index of Row
   * @return short data type data
   */
  public short getShort(int ordinal) {
    return (short) data[ordinal];
  }

  /**
   * get int data type data by ordinal
   *
   * @param ordinal the data index of Row
   * @return int data type data
   */
  public int getInt(int ordinal) {
    return (Integer) data[ordinal];
  }

  /**
   * get long data type data by ordinal
   *
   * @param ordinal the data index of Row
   * @return long data type data
   */
  public long getLong(int ordinal) {
    return (long) data[ordinal];
  }

  /**
   * get array data type data by ordinal
   *
   * @param ordinal the data index of Row
   * @return array data type data
   */
  public Object[] getArray(int ordinal) {
    return (Object[]) data[ordinal];
  }

  /**
   * get double data type data by ordinal
   *
   * @param ordinal the data index of Row
   * @return double data type data
   */
  public double getDouble(int ordinal) {
    return (double) data[ordinal];
  }

  /**
   * get boolean data type data by ordinal
   *
   * @param ordinal the data index of Row
   * @return boolean data type data
   */
  public boolean getBoolean(int ordinal) {
    return (boolean) data[ordinal];
  }

  /**
   * get byte data type data by ordinal
   *
   * @param ordinal the data index of Row
   * @return byte data type data
   */
  public Byte getByte(int ordinal) {
    return (Byte) data[ordinal];
  }

  /**
   * get float data type data by ordinal
   *
   * @param ordinal the data index of Row
   * @return float data type data
   */
  public float getFloat(int ordinal) {
    return (float) data[ordinal];
  }

  /**
   * get varchar data type data by ordinal
   * This is for CSDK
   * JNI don't support varchar, so carbon convert decimal to string
   *
   * @param ordinal the data index of Row
   * @return string data type data
   */
  public String getVarchar(int ordinal) {
    return (String) data[ordinal];
  }

  /**
   * get decimal data type data by ordinal
   * This is for CSDK
   * JNI don't support Decimal, so carbon convert decimal to string
   *
   * @param ordinal the data index of Row
   * @return string data type data
   */
  public String getDecimal(int ordinal) {
    return ((BigDecimal) data[ordinal]).toString();
  }

  /**
   * get data type by ordinal
   *
   * @param ordinal the data index of Row
   * @return data type
   */
  public DataType getDataType(int ordinal) {
    return dataTypes[ordinal];
  }

  /**
   * get data type name by ordinal
   *
   * @param ordinal the data index of Row
   * @return data type name
   */
  public String getDataTypeName(int ordinal) {
    return dataTypes[ordinal].getName();
  }

  /**
   * get element type name by ordinal
   * child schema data type name
   * for example: return STRING if it's Array<String> in java
   *
   * @param ordinal the data index of Row
   * @return element type name
   */
  public String getArrayElementTypeName(int ordinal) {
    if (getDataTypeName(ordinal).equalsIgnoreCase("ARRAY")) {
      return ((ArrayType) dataTypes[ordinal]).getElementType().getName();
    }
    throw new RuntimeException("Only support Array type.");
  }

  /**
   * get carbon row length by ordinal
   *
   * @return carbon row length
   */
  public int getLength() {
    return this.data.length;
  }

  public Object getObject(int ordinal) {
    return data[ordinal];
  }

  @Override public String toString() {
    return Arrays.toString(data);
  }
}
