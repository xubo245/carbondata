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

package org.apache.carbondata.core.metadata.datatype;

public class ArrayType extends DataType {

  private DataType elementType;

  private String elementName;

  ArrayType(DataType elementType) {
    super(DataTypes.ARRAY_TYPE_ID, 9, "ARRAY", -1);
    this.elementType = elementType;
  }

  public ArrayType(DataType elementType, String elementName) {
    super(DataTypes.ARRAY_TYPE_ID, 9, "ARRAY", -1);
    this.elementType = elementType;
    this.elementName = elementName;
  }

  @Override
  public boolean isComplexType() {
    return true;
  }

  @Override
  public boolean equals(Object obj) {
    if (this == obj) {
      return true;
    }
    if (obj == null) {
      return false;
    }
    if (getClass() != obj.getClass()) {
      return false;
    }
    ArrayType other = (ArrayType) obj;
    if (!this.getName().equalsIgnoreCase(other.getName())) {
      return false;
    }
    if (!this.getElementType().equals(other.getElementType())) {
      return false;
    }
    return true;
  }

  @Override
  public int hashCode() {
    final int prime = 31;
    int result = 1;
    result = prime * result + getName().hashCode();
    result = prime * result + getElementType().hashCode();
    return result;
  }

  public DataType getElementType() {
    return elementType;
  }

  public String getElementName() {
    return elementName;
  }

  public void setElementName(String elementName) {
    this.elementName = elementName;
  }
}
