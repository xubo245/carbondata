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

package org.apache.carbondata.core.datastore.page.encoding.bool;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.apache.carbondata.core.constants.CarbonCommonConstants;
import org.apache.carbondata.core.datastore.compression.Compressor;
import org.apache.carbondata.core.datastore.compression.CompressorFactory;
import org.apache.carbondata.core.datastore.page.ColumnPage;
import org.apache.carbondata.core.datastore.page.LazyColumnPage;
import org.apache.carbondata.core.datastore.page.encoding.ColumnPageCodec;
import org.apache.carbondata.core.datastore.page.encoding.ColumnPageDecoder;
import org.apache.carbondata.core.datastore.page.encoding.ColumnPageEncoder;
import org.apache.carbondata.core.datastore.page.encoding.ColumnPageEncoderMeta;
import org.apache.carbondata.core.memory.MemoryException;
import org.apache.carbondata.core.metadata.datatype.DataType;
import org.apache.carbondata.format.Encoding;

/**
 * boolean data type strategy will select encoding base on column page data type and statistics
 */
public class BooleanPageCodec implements ColumnPageCodec {
  private String name = "BooleanPageCodec";
  private DataType dataType;

  public BooleanPageCodec() {
  }

  @Override
  public String getName() {
    return name;
  }

  public BooleanPageCodec(DataType dataType) {
    this.dataType = dataType;
  }

  @Override
  public ColumnPageEncoder createEncoder(Map<String, String> parameter) {
    return new BooleanCompressor(CarbonCommonConstants.DEFAULT_COMPRESSOR);
  }

  @Override
  public ColumnPageDecoder createDecoder(ColumnPageEncoderMeta meta) {
    assert meta instanceof BooleanEncoderMeta;
    BooleanEncoderMeta codecMeta = (BooleanEncoderMeta) meta;
    return new BooleanPageCodec.BooleanDecompressor(meta);

  }

  private static class BooleanCompressor extends ColumnPageEncoder {

    private Compressor compressor;

    BooleanCompressor(String compressorName) {
      this.compressor = CompressorFactory.getInstance().getCompressor(compressorName);
    }

    @Override
    protected byte[] encodeData(ColumnPage input) throws MemoryException, IOException {
      return input.compress(compressor);
    }

    @Override
    protected List<Encoding> getEncodingList() {
      List<Encoding> encodings = new ArrayList<>();
      encodings.add(Encoding.BOOL_BYTE);
      return encodings;
    }

    @Override
    protected ColumnPageEncoderMeta getEncoderMeta(ColumnPage inputPage) {
      return new BooleanEncoderMeta(inputPage.getColumnSpec(), inputPage.getDataType(),
          inputPage.getStatistics(),compressor.getName());
    }

  }

  private class BooleanDecompressor implements ColumnPageDecoder {

    private Compressor compressor;
    private int scale;
    private int precision;

    BooleanDecompressor(String compressorName, int scale, int precision) {
      this.compressor = CompressorFactory.getInstance().getCompressor(compressorName);
      this.scale = scale;
      this.precision = precision;
    }
    private ColumnPageEncoderMeta meta;

    BooleanDecompressor(ColumnPageEncoderMeta meta) {
      this.meta = meta;
    }

    @Override
    public ColumnPage decode(byte[] input, int offset, int length) throws MemoryException {
      ColumnPage decodedPage;
      if (dataType == DataType.DECIMAL) {
        decodedPage = ColumnPage.decompressDecimalPage(meta, input, offset, length);
      } else {
        decodedPage = ColumnPage.decompress(meta, input, offset, length);
      }
      return decodedPage;
    }
  }

}
