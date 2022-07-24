package com.wojustme.goblin.storage.io;

import com.google.common.base.Preconditions;
import com.wojustme.goblin.meta.catalog.model.CatalogColumn;
import com.wojustme.goblin.storage.TableIO;
import com.wojustme.goblin.storage.impl.TableDesc;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.ipc.ArrowFileReader;
import org.apache.arrow.vector.ipc.ArrowFileWriter;
import org.apache.arrow.vector.ipc.message.ArrowBlock;
import org.apache.arrow.vector.util.ByteArrayReadableSeekableByteChannel;
import org.apache.commons.io.FileUtils;

import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.channels.Channels;
import java.util.List;

public class SimpleTableIO implements TableIO {

  private final TableDesc tableDesc;

  public SimpleTableIO(TableDesc tableDesc) {
    this.tableDesc = tableDesc;
  }

  @Override
  public List<FieldVector> readColumns(String... col) {
    return null;
    //    for (int i = 0; i < col.length; i++) {
    //      final File colFile = FileUtils.getFile(tableDesc.tableDir, col[i]);
    //      Preconditions.checkArgument(colFile.exists() && colFile.isFile());
    //
    //      final BufferAllocator allocator = new RootAllocator();
    //      try (ArrowFileReader reader =
    //          new ArrowFileReader(
    //              new ByteArrayReadableSeekableByteChannel(out.toByteArray()), allocator)) {
    //
    //        // read the 4-th batch
    //        ArrowBlock block = reader.getRecordBlocks().get(3);
    //        reader.loadRecordBatch(block);
    //        VectorSchemaRoot readBatch = reader.getVectorSchemaRoot();
    //      }
    //    }
  }

  @Override
  public void write(List<FieldVector> valueVectors) {
    final List<CatalogColumn> columns = tableDesc.getCatalogTable().columns;
    for (int i = 0; i < columns.size(); i++) {
      final CatalogColumn column = columns.get(i);
      try {
        final File columnFile = checkAndCreateColumnFile(column.fieldName());
        final FileOutputStream out = new FileOutputStream(columnFile, true);
        ArrowFileWriter writer =
            new ArrowFileWriter(
                new VectorSchemaRoot(valueVectors.get(0)), null, Channels.newChannel(out));
        writer.start();
        writer.writeBatch();
        writer.end();
      } catch (Exception e) {
        throw new RuntimeException("Write col " + column.fieldName() + " error.", e);
      }
    }
  }

  private File checkAndCreateColumnFile(String columnName) throws IOException {
    final File colFile = FileUtils.getFile(tableDesc.tableDir, columnName);
    if (colFile.exists()) {
      Preconditions.checkArgument(colFile.isFile());
    }
    colFile.createNewFile();
    return colFile;
  }
}
