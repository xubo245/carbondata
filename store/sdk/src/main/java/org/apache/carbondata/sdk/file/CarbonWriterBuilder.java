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

package org.apache.carbondata.sdk.file;

import java.io.IOException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Objects;

import org.apache.carbondata.common.Strings;
import org.apache.carbondata.common.annotations.InterfaceAudience;
import org.apache.carbondata.common.annotations.InterfaceStability;
import org.apache.carbondata.common.exceptions.sql.InvalidLoadOptionException;
import org.apache.carbondata.core.datastore.impl.FileFactory;
import org.apache.carbondata.core.metadata.CarbonMetadata;
import org.apache.carbondata.core.metadata.converter.SchemaConverter;
import org.apache.carbondata.core.metadata.converter.ThriftWrapperSchemaConverterImpl;
import org.apache.carbondata.core.metadata.datatype.StructField;
import org.apache.carbondata.core.metadata.schema.table.CarbonTable;
import org.apache.carbondata.core.metadata.schema.table.TableInfo;
import org.apache.carbondata.core.metadata.schema.table.TableSchema;
import org.apache.carbondata.core.metadata.schema.table.TableSchemaBuilder;
import org.apache.carbondata.core.util.path.CarbonTablePath;
import org.apache.carbondata.core.writer.ThriftWriter;
import org.apache.carbondata.processing.loading.model.CarbonLoadModel;
import org.apache.carbondata.processing.loading.model.CarbonLoadModelBuilder;

/**
 * Biulder for {@link CarbonWriter}
 */
@InterfaceAudience.User
@InterfaceStability.Unstable
public class CarbonWriterBuilder {
  private Schema schema;
  private String path;
  private String[] sortColumns;
  private boolean persistSchemaFile;
  private int blockletSize;
  private int blockSize;
  private boolean isUnManagedTable;
  private long UUID;

  /**
   * prepares the builder with the schema provided
   * @param schema is instance of Schema
   * @return updated CarbonWriterBuilder
   */
  public CarbonWriterBuilder withSchema(Schema schema) {
    Objects.requireNonNull(schema, "schema should not be null");
    this.schema = schema;
    return this;
  }

  /**
   * Sets the output path of the writer builder
   * @param path is the absolute path where output files are written
   * @return updated CarbonWriterBuilder
   */
  public CarbonWriterBuilder outputPath(String path) {
    Objects.requireNonNull(path, "path should not be null");
    this.path = path;
    return this;
  }

  /**
   * sets the list of columns that needs to be in sorted order
   * @param sortColumns is a string array of columns that needs to be sorted
   * @return updated CarbonWriterBuilder
   */
  public CarbonWriterBuilder sortBy(String[] sortColumns) {
    this.sortColumns = sortColumns;
    return this;
  }

  /**
   * If set, create a schema file in metadata folder.
   * @param persist is a boolean value, If set, create a schema file in metadata folder
   * @return updated CarbonWriterBuilder
   */
  public CarbonWriterBuilder persistSchemaFile(boolean persist) {
    this.persistSchemaFile = persist;
    return this;
  }

  /**
   * If set true, writes the carbondata and carbonindex files in a flat folder structure
   * @param isUnManagedTable is a boolelan value if set writes
   *                     the carbondata and carbonindex files in a flat folder structure
   * @return updated CarbonWriterBuilder
   */
  public CarbonWriterBuilder unManagedTable(boolean isUnManagedTable) {
    Objects.requireNonNull(isUnManagedTable, "UnManaged Table should not be null");
    this.isUnManagedTable = isUnManagedTable;
    return this;
  }

  /**
   * to set the timestamp in the carbondata and carbonindex index files
   * @param UUID is a timestamp to be used in the carbondata and carbonindex index files
   * @return updated CarbonWriterBuilder
   */
  public CarbonWriterBuilder uniqueIdentifier(long UUID) {
    Objects.requireNonNull(UUID, "Unique Identifier should not be null");
    this.UUID = UUID;
    return this;
  }

  /**
   * To set the carbondata file size in MB between 1MB-2048MB
   * @param blockSize is size in MB between 1MB to 2048 MB
   * @return updated CarbonWriterBuilder
   */
  public CarbonWriterBuilder withBlockSize(int blockSize) {
    if (blockSize <= 0 || blockSize > 2048) {
      throw new IllegalArgumentException("blockSize should be between 1 MB to 2048 MB");
    }
    this.blockSize = blockSize;
    return this;
  }

  /**
   * To set the blocklet size of carbondata file
   * @param blockletSize is blocklet size in MB
   * @return updated CarbonWriterBuilder
   */
  public CarbonWriterBuilder withBlockletSize(int blockletSize) {
    if (blockletSize <= 0) {
      throw new IllegalArgumentException("blockletSize should be greater than zero");
    }
    this.blockletSize = blockletSize;
    return this;
  }

  /**
   * Build a {@link CarbonWriter}, which accepts row in CSV format
   */
  public CarbonWriter buildWriterForCSVInput() throws IOException, InvalidLoadOptionException {
    Objects.requireNonNull(schema, "schema should not be null");
    Objects.requireNonNull(path, "path should not be null");
    CarbonLoadModel loadModel = createLoadModel();
    return new CSVCarbonWriter(loadModel);
  }

  /**
   * Build a {@link CarbonWriter}, which accepts Avro object
   * @return
   * @throws IOException
   */
  public CarbonWriter buildWriterForAvroInput() throws IOException, InvalidLoadOptionException {
    Objects.requireNonNull(schema, "schema should not be null");
    Objects.requireNonNull(path, "path should not be null");
    CarbonLoadModel loadModel = createLoadModel();
    return new AvroCarbonWriter(loadModel);
  }

  private CarbonLoadModel createLoadModel() throws IOException, InvalidLoadOptionException {
    // build CarbonTable using schema
    CarbonTable table = buildCarbonTable();
    if (persistSchemaFile) {
      // we are still using the traditional carbon table folder structure
      persistSchemaFile(table, CarbonTablePath.getSchemaFilePath(path));
    }

    // build LoadModel
    return buildLoadModel(table, UUID);
  }

  /**
   * Build a {@link CarbonTable}
   */
  private CarbonTable buildCarbonTable() {
    TableSchemaBuilder tableSchemaBuilder = TableSchema.builder();
    if (blockSize > 0) {
      tableSchemaBuilder = tableSchemaBuilder.blockSize(blockSize);
    }

    if (blockletSize > 0) {
      tableSchemaBuilder = tableSchemaBuilder.blockletSize(blockletSize);
    }

    List<String> sortColumnsList;
    if (sortColumns != null) {
      sortColumnsList = Arrays.asList(sortColumns);
    } else {
      sortColumnsList = new LinkedList<>();
    }
    for (Field field : schema.getFields()) {
      tableSchemaBuilder.addColumn(
          new StructField(field.getFieldName(), field.getDataType()),
          sortColumnsList.contains(field.getFieldName()));
    }
    String tableName;
    String dbName;
    if (!isUnManagedTable) {
      tableName = "_tempTable";
      dbName = "_tempDB";
    } else {
      dbName = null;
      tableName = null;
    }
    TableSchema schema = tableSchemaBuilder.build();
    schema.setTableName(tableName);
    CarbonTable table = CarbonTable.builder()
        .tableName(schema.getTableName())
        .databaseName(dbName)
        .tablePath(path)
        .tableSchema(schema)
        .isUnManagedTable(isUnManagedTable)
        .build();
    return table;
  }

  /**
   * Save the schema of the {@param table} to {@param persistFilePath}
   * @param table table object containing schema
   * @param persistFilePath absolute file path with file name
   */
  private void persistSchemaFile(CarbonTable table, String persistFilePath) throws IOException {
    TableInfo tableInfo = table.getTableInfo();
    String schemaMetadataPath = CarbonTablePath.getFolderContainingFile(persistFilePath);
    CarbonMetadata.getInstance().loadTableMetadata(tableInfo);
    SchemaConverter schemaConverter = new ThriftWrapperSchemaConverterImpl();
    org.apache.carbondata.format.TableInfo thriftTableInfo =
        schemaConverter.fromWrapperToExternalTableInfo(
            tableInfo,
            tableInfo.getDatabaseName(),
            tableInfo.getFactTable().getTableName());
    org.apache.carbondata.format.SchemaEvolutionEntry schemaEvolutionEntry =
        new org.apache.carbondata.format.SchemaEvolutionEntry(
            tableInfo.getLastUpdatedTime());
    thriftTableInfo.getFact_table().getSchema_evolution().getSchema_evolution_history()
        .add(schemaEvolutionEntry);
    FileFactory.FileType fileType = FileFactory.getFileType(schemaMetadataPath);
    if (!FileFactory.isFileExist(schemaMetadataPath, fileType)) {
      FileFactory.mkdirs(schemaMetadataPath, fileType);
    }
    ThriftWriter thriftWriter = new ThriftWriter(persistFilePath, false);
    thriftWriter.open();
    thriftWriter.write(thriftTableInfo);
    thriftWriter.close();
  }

  /**
   * Build a {@link CarbonLoadModel}
   */
  private CarbonLoadModel buildLoadModel(CarbonTable table, long UUID)
      throws InvalidLoadOptionException, IOException {
    Map<String, String> options = new HashMap<>();
    if (sortColumns != null) {
      options.put("sort_columns", Strings.mkString(sortColumns, ","));
    }
    CarbonLoadModelBuilder builder = new CarbonLoadModelBuilder(table);
    return builder.build(options, UUID);
  }
}