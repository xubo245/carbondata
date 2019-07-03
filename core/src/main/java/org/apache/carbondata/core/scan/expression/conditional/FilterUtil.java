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

package org.apache.carbondata.core.scan.expression.conditional;

import java.util.List;

import org.apache.carbondata.core.metadata.datatype.*;
import org.apache.carbondata.core.scan.expression.ColumnExpression;
import org.apache.carbondata.core.scan.expression.Expression;
import org.apache.carbondata.core.scan.expression.LiteralExpression;
import org.apache.carbondata.core.scan.expression.logical.OrExpression;


/**
 * provide function to prepare expression for filter
 */
public class FilterUtil {
  public static Expression prepareEqualToExpression(String columnName, String dataType,
      Object value) {
    if (DataTypes.STRING.getName().equalsIgnoreCase(dataType)) {
      return new EqualToExpression(
          new ColumnExpression(columnName, DataTypes.STRING),
          new LiteralExpression(value, DataTypes.STRING));
    } else if (DataTypes.INT.getName().equalsIgnoreCase(dataType)) {
      return new EqualToExpression(
          new ColumnExpression(columnName, DataTypes.INT),
          new LiteralExpression(value, DataTypes.INT));
    } else if (DataTypes.DOUBLE.getName().equalsIgnoreCase(dataType)) {
      return new EqualToExpression(
          new ColumnExpression(columnName, DataTypes.DOUBLE),
          new LiteralExpression(value, DataTypes.DOUBLE));
    } else if (DataTypes.FLOAT.getName().equalsIgnoreCase(dataType)) {
      return new EqualToExpression(
          new ColumnExpression(columnName, DataTypes.FLOAT),
          new LiteralExpression(value, DataTypes.FLOAT));
    } else if (DataTypes.SHORT.getName().equalsIgnoreCase(dataType)) {
      return new EqualToExpression(
          new ColumnExpression(columnName, DataTypes.SHORT),
          new LiteralExpression(value, DataTypes.SHORT));
    } else if (DataTypes.BINARY.getName().equalsIgnoreCase(dataType)) {
      return new EqualToExpression(
          new ColumnExpression(columnName, DataTypes.BINARY),
          new LiteralExpression(value, DataTypes.BINARY));
    } else if (DataTypes.DATE.getName().equalsIgnoreCase(dataType)) {
      return new EqualToExpression(
          new ColumnExpression(columnName, DataTypes.DATE),
          new LiteralExpression(value, DataTypes.DATE));
    } else if (DataTypes.LONG.getName().equalsIgnoreCase(dataType)) {
      return new EqualToExpression(
          new ColumnExpression(columnName, DataTypes.LONG),
          new LiteralExpression(value, DataTypes.LONG));
    } else if (DataTypes.TIMESTAMP.getName().equalsIgnoreCase(dataType)) {
      return new EqualToExpression(
          new ColumnExpression(columnName, DataTypes.TIMESTAMP),
          new LiteralExpression(value, DataTypes.TIMESTAMP));
    } else if (DataTypes.BYTE.getName().equalsIgnoreCase(dataType)) {
      return new EqualToExpression(
          new ColumnExpression(columnName, DataTypes.BYTE),
          new LiteralExpression(value, DataTypes.BYTE));
    } else {
      throw new IllegalArgumentException("Unsupported data type: " + dataType);
    }
  }

  public static Expression prepareEqualToExpressionSet(String columnName, DataType dataType,
      List<Object> values) {
    Expression expression = null;
    if (0 == values.size()) {
      expression = prepareEqualToExpression(columnName, dataType.getName(), null);
    } else {
      expression = prepareEqualToExpression(columnName, dataType.getName(), values.get(0));
    }
    for (int i = 1; i < values.size(); i++) {
      Expression expression2 = prepareEqualToExpression(columnName,
          dataType.getName(), values.get(i));
      expression = new OrExpression(expression, expression2);
    }
    return expression;
  }

  public static Expression prepareEqualToExpressionSet(String columnName, String dataType,
      List<Object> values) {
    if (DataTypes.STRING.getName().equalsIgnoreCase(dataType)) {
      return prepareEqualToExpressionSet(columnName, DataTypes.STRING, values);
    } else if (DataTypes.INT.getName().equalsIgnoreCase(dataType)) {
      return prepareEqualToExpressionSet(columnName, DataTypes.INT, values);
    } else if (DataTypes.DOUBLE.getName().equalsIgnoreCase(dataType)) {
      return prepareEqualToExpressionSet(columnName, DataTypes.DOUBLE, values);
    } else if (DataTypes.FLOAT.getName().equalsIgnoreCase(dataType)) {
      return prepareEqualToExpressionSet(columnName, DataTypes.FLOAT, values);
    } else if (DataTypes.SHORT.getName().equalsIgnoreCase(dataType)) {
      return prepareEqualToExpressionSet(columnName, DataTypes.SHORT, values);
    } else if (DataTypes.BINARY.getName().equalsIgnoreCase(dataType)) {
      return prepareEqualToExpressionSet(columnName, DataTypes.BINARY, values);
    } else if (DataTypes.DATE.getName().equalsIgnoreCase(dataType)) {
      return prepareEqualToExpressionSet(columnName, DataTypes.DATE, values);
    } else if (DataTypes.LONG.getName().equalsIgnoreCase(dataType)) {
      return prepareEqualToExpressionSet(columnName, DataTypes.LONG, values);
    } else if (DataTypes.TIMESTAMP.getName().equalsIgnoreCase(dataType)) {
      return prepareEqualToExpressionSet(columnName, DataTypes.TIMESTAMP, values);
    } else if (DataTypes.BYTE.getName().equalsIgnoreCase(dataType)) {
      return prepareEqualToExpressionSet(columnName, DataTypes.BYTE, values);
    } else {
      throw new IllegalArgumentException("Unsupported data type: " + dataType);
    }
  }
}
