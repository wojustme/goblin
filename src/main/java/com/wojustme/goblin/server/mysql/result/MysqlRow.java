package com.wojustme.goblin.server.mysql.result;

import com.google.common.base.Preconditions;
import com.wojustme.goblin.server.mysql.packet.ResultSetRowPacket;

import java.util.ArrayList;
import java.util.List;

public class MysqlRow {

  private MysqlResult mysqlResult;

  private List<String> datas = new ArrayList<>();

  private boolean finish = false;

   MysqlRow(MysqlResult mysqlResult) {
    this.mysqlResult = mysqlResult;
  }

  public MysqlRow putInt(Integer value) {
    datas.add(value.toString());
    return this;
  }

  public MysqlRow putString(String value) {
    datas.add(value);
    return this;
  }

  public MysqlResult finish() {
    Preconditions.checkArgument(
        datas.size() == mysqlResult.getFieldCount(),
        "Size of row datas isn't equal to field's length.");
    finish = true;
    return mysqlResult;
  }

  public ResultSetRowPacket genResultSetRowPacket(int sequenceId) {
    Preconditions.checkArgument(finish, "Current row container should be called finished.");
    return new ResultSetRowPacket(sequenceId, datas);
  }
}
