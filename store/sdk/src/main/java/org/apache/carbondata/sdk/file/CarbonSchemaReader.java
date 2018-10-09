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

import java.io.DataInputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;

import org.apache.carbondata.core.constants.CarbonCommonConstants;
import org.apache.carbondata.core.datastore.FileReader;
import org.apache.carbondata.core.datastore.filesystem.CarbonFile;
import org.apache.carbondata.core.datastore.filesystem.CarbonFileFilter;
import org.apache.carbondata.core.datastore.impl.FileFactory;
import org.apache.carbondata.core.metadata.converter.SchemaConverter;
import org.apache.carbondata.core.metadata.converter.ThriftWrapperSchemaConverterImpl;
import org.apache.carbondata.core.metadata.schema.table.column.ColumnSchema;
import org.apache.carbondata.core.reader.CarbonFooterReaderV3;
import org.apache.carbondata.core.reader.CarbonHeaderReader;
import org.apache.carbondata.core.reader.CarbonIndexFileReader;
import org.apache.carbondata.core.util.CarbonUtil;
import org.apache.carbondata.core.util.path.CarbonTablePath;
import org.apache.carbondata.format.FileFooter3;

import static org.apache.carbondata.core.util.CarbonUtil.thriftColumnSchemaToWrapperColumnSchema;

/**
 * Schema reader for carbon files, including carbondata file, carbonindex file, and schema file
 */
public class CarbonSchemaReader {

  /**
   * Read schema file and return the schema
   *
   * @param schemaFilePath complete path including schema file name
   * @return schema object
   * @throws IOException
   */
  @Deprecated
  public static Schema readSchemaInSchemaFile(String schemaFilePath) throws IOException {
    org.apache.carbondata.format.TableInfo tableInfo = CarbonUtil.readSchemaFile(schemaFilePath);
    SchemaConverter schemaConverter = new ThriftWrapperSchemaConverterImpl();
    List<ColumnSchema> schemaList = schemaConverter
        .fromExternalToWrapperTableInfo(tableInfo, "", "", "")
        .getFactTable()
        .getListOfColumns();
    return new Schema(schemaList);
  }

  /**
   * Read carbondata file and return the schema
   *
   * @param path carbondata store path
   * @return Schema object
   * @throws IOException
   */
  public static Schema readSchemaFromFirstDataFile(String path) throws IOException {
    String dataFilePath = getFirstCarbonDataFile(path);
    return readSchemaInDataFile(dataFilePath);
  }

  /**
   * get first carbondata file in path and don't check all files schema
   *
   * @param path carbondata file path
   * @return first carbondata file name
   */
  public static String getFirstCarbonDataFile(String path) {
    String dataFilePath = path;
    if (!(dataFilePath.contains(".carbondata"))) {
      CarbonFile[] carbonFiles = FileFactory
          .getCarbonFile(path)
          .listFiles(new CarbonFileFilter() {
            @Override
            public boolean accept(CarbonFile file) {
              if (file == null) {
                return false;
              }
              return file.getName().endsWith(".carbondata");
            }
          });
      if (carbonFiles == null || carbonFiles.length < 1) {
        throw new RuntimeException("Carbon data file not exists.");
      }
      dataFilePath = carbonFiles[0].getAbsolutePath();
    }
    return dataFilePath;
  }

  /**
   * Read carbondata file and return the schema
   *
   * @param path carbonindex store path
   * @return Schema object
   * @throws IOException
   */
  public static Schema readSchemaInDataFile(String path) throws IOException {
    String dataFilePath = path;
    if (!(dataFilePath.contains(".carbondata"))) {
      CarbonFile[] carbonFiles = FileFactory
          .getCarbonFile(path)
          .listFiles(new CarbonFileFilter() {
            @Override
            public boolean accept(CarbonFile file) {
              if (file == null) {
                return false;
              }
              return file.getName().endsWith(".carbondata");
            }
          });
      if (carbonFiles == null || carbonFiles.length < 1) {
        throw new RuntimeException("Carbon data file not exists.");
      }
      dataFilePath = carbonFiles[0].getAbsolutePath();
    }

    CarbonHeaderReader reader = new CarbonHeaderReader(dataFilePath);
    List<ColumnSchema> columnSchemaList = new ArrayList<ColumnSchema>();
    List<ColumnSchema> schemaList = reader.readSchema();
    for (int i = 0; i < schemaList.size(); i++) {
      ColumnSchema columnSchema = schemaList.get(i);
      if (!(columnSchema.getColumnName().contains("."))) {
        columnSchemaList.add(columnSchema);
      }
    }
    return new Schema(columnSchemaList);
  }

  /**
   * This method return the version details in formatted string by reading from carbondata file
   * @param dataFilePath
   * @return
   * @throws IOException
   */
  public static String getVersionDetails(String dataFilePath) throws IOException {
    long fileSize =
        FileFactory.getCarbonFile(dataFilePath, FileFactory.getFileType(dataFilePath)).getSize();
    FileReader fileReader = FileFactory.getFileHolder(FileFactory.getFileType(dataFilePath));
    ByteBuffer buffer =
        fileReader.readByteBuffer(FileFactory.getUpdatedFilePath(dataFilePath), fileSize - 8, 8);
    CarbonFooterReaderV3 footerReader = new CarbonFooterReaderV3(dataFilePath, buffer.getLong());
    FileFooter3 footer = footerReader.readFooterVersion3();
    if (null != footer.getExtra_info()) {
      return footer.getExtra_info().get(CarbonCommonConstants.CARBON_WRITTEN_BY_FOOTER_INFO)
          + " in version: " + footer.getExtra_info()
          .get(CarbonCommonConstants.CARBON_WRITTEN_VERSION);
    } else {
      return "Version Details are not found in carbondata file";
    }
  }

  /**
   * Read carbonindex file and return the schema
   *
   * @param path complete path including index file name
   * @return schema object
   * @throws IOException
   */
  public static Schema readSchemaFromFirstIndexFile(String path) throws IOException {
    String dataFilePath = getFirstCarbonIndexFile(path);
    return readSchemaInIndexFile(dataFilePath);
  }

  /**
   * get first carbonindex file in path and don't check all files schema
   *
   * @param path carbonindex file path
   * @return first carbonindex file name
   */
  public static String getFirstCarbonIndexFile(String path) {
    String indexFilePath = path;
    if (!(indexFilePath.contains(".carbonindex"))) {
      CarbonFile[] carbonFiles = FileFactory
          .getCarbonFile(path)
          .listFiles(new CarbonFileFilter() {
            @Override
            public boolean accept(CarbonFile file) {
              if (file == null) {
                return false;
              }
              return file.getName().endsWith(".carbonindex");
            }
          });
      if (carbonFiles == null || carbonFiles.length < 1) {
        throw new RuntimeException("Carbon index file not exists.");
      }
      indexFilePath = carbonFiles[0].getAbsolutePath();
    }
    return indexFilePath;
  }

  /**
   * Read carbonindex file and return the schema
   *
   * @param indexFilePath complete path including index file name
   * @return schema object
   * @throws IOException
   */
  public static Schema readSchemaInIndexFile(String indexFilePath) throws IOException {
    CarbonFile indexFile =
        FileFactory.getCarbonFile(indexFilePath, FileFactory.getFileType(indexFilePath));
    if (!indexFile.getName().endsWith(CarbonTablePath.INDEX_FILE_EXT)) {
      throw new IOException("Not an index file name");
    }
    // read schema from the first index file
    DataInputStream dataInputStream =
        FileFactory.getDataInputStream(indexFilePath, FileFactory.getFileType(indexFilePath));
    byte[] bytes = new byte[(int) indexFile.getSize()];
    try {
      //get the file in byte buffer
      dataInputStream.readFully(bytes);
      CarbonIndexFileReader indexReader = new CarbonIndexFileReader();
      // read from byte buffer.
      indexReader.openThriftReader(bytes);
      // get the index header
      org.apache.carbondata.format.IndexHeader readIndexHeader = indexReader.readIndexHeader();
      List<ColumnSchema> columnSchemaList = new ArrayList<ColumnSchema>();
      List<org.apache.carbondata.format.ColumnSchema> table_columns =
          readIndexHeader.getTable_columns();
      for (org.apache.carbondata.format.ColumnSchema columnSchema : table_columns) {
        if (!(columnSchema.column_name.contains("."))) {
          columnSchemaList.add(thriftColumnSchemaToWrapperColumnSchema(columnSchema));
        }
      }
      return new Schema(columnSchemaList);
    } finally {
      dataInputStream.close();
    }
  }

}
