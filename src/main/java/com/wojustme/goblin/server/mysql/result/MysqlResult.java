package com.wojustme.goblin.server.mysql.result;

import com.google.common.base.Preconditions;
import com.wojustme.goblin.server.mysql.packet.ColumnCountPacket;
import com.wojustme.goblin.server.mysql.packet.EofResponsePacket;
import com.wojustme.goblin.server.mysql.protocol.ColumnType;
import io.netty.channel.ChannelHandlerContext;

import java.util.ArrayList;
import java.util.List;

public class MysqlResult {

  private List<MysqlField> fields = new ArrayList<>();

  private int fieldCount = 0;

  private List<MysqlRow> rows = new ArrayList<>();

  private boolean stopUpdateField = false;

  private MysqlResult() {}

  public static MysqlResult create() {
    return new MysqlResult();
  }

  public MysqlResult addField(String name, ColumnType columnType) {
    Preconditions.checkArgument(!stopUpdateField, "Shouldn't update field info now.");
    fields.add(new MysqlField(name, columnType));
    fieldCount++;
    return this;
  }

  public MysqlRow createRow() {
    stopUpdateField = true;
    final MysqlRow row = new MysqlRow(this);
    rows.add(row);
    return row;
  }

  public List<MysqlField> getFields() {
    return fields;
  }

  public int getFieldCount() {
    return fieldCount;
  }

  public List<MysqlRow> getRows() {
    return rows;
  }

  public void flush(ChannelHandlerContext ctx, int sequenceId) {
    ctx.write(new ColumnCountPacket(++sequenceId, fieldCount));

    for (MysqlField field : fields) {
      ctx.write(field.genColumnDefPacket(++sequenceId));
    }

    ctx.write(new EofResponsePacket(++sequenceId, 0));

    for (MysqlRow row : rows) {
      ctx.write(row.genResultSetRowPacket(++sequenceId));
    }
    ctx.writeAndFlush(new EofResponsePacket(++sequenceId, 0));
  }
}
