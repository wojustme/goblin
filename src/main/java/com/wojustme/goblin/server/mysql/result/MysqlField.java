package com.wojustme.goblin.server.mysql.result;


import com.wojustme.goblin.server.mysql.packet.ColumnDefinitionPacket;
import com.wojustme.goblin.server.mysql.protocol.ColumnType;

public record MysqlField(String name, ColumnType columnType) {
  public ColumnDefinitionPacket genColumnDefPacket(int sequenceId) {
    return ColumnDefinitionPacket.builder()
            .sequenceId(sequenceId)
            .name(name)
            .orgName(name)
            .columnType(columnType)
            .build();
  }
}


