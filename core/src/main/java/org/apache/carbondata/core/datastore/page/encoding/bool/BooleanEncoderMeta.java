package org.apache.carbondata.core.datastore.page.encoding.bool;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.carbondata.core.datastore.TableSpec;
import org.apache.carbondata.core.datastore.page.encoding.ColumnPageEncoderMeta;
import org.apache.carbondata.core.datastore.page.statistics.SimpleStatsResult;
import org.apache.carbondata.core.metadata.datatype.DataType;
import org.apache.carbondata.core.metadata.schema.table.Writable;

public class BooleanEncoderMeta extends ColumnPageEncoderMeta implements Writable {
  private String compressorName;

  public BooleanEncoderMeta() {
  }

  public BooleanEncoderMeta(TableSpec.ColumnSpec columnSpec, DataType storeDataType,
                            SimpleStatsResult stats, String compressorName) {
    super(columnSpec,storeDataType,stats,compressorName);
    this.compressorName = compressorName;
  }

  public String getCompressorName() {
    return compressorName;
  }

  @Override
  public void write(DataOutput out) throws IOException {
    super.write(out);
    out.writeUTF(compressorName);
  }

  @Override
  public void readFields(DataInput in) throws IOException {
    super.readFields(in);
    compressorName = in.readUTF();
  }
}
