package com.wojustme.goblin.server.mysql.packet;


import com.wojustme.goblin.server.mysql.protocol.CharacterSet;
import com.wojustme.goblin.server.mysql.protocol.ColumnFlag;
import com.wojustme.goblin.server.mysql.protocol.ColumnType;

import java.util.Collections;
import java.util.EnumSet;
import java.util.Set;

public class ColumnDefinitionPacket extends MysqlPacket {

  public final String catalog;

  public final String schema;

  public final String table;

  public final String orgTable;

  public final String name;

  public final String orgName;

  public final CharacterSet characterSet;

  public final long columnLength;

  public final ColumnType columnType;

  public final Set<ColumnFlag> flags;

  public final int decimals;

  private ColumnDefinitionPacket(Builder builder) {
    super(builder.sequenceId);
    this.catalog = builder.catalog;
    this.schema = builder.schema;
    this.table = builder.table;
    this.orgTable = builder.orgTable;
    this.name = builder.name;
    this.orgName = builder.orgName;
    this.characterSet = builder.characterSet;
    this.columnLength = builder.columnLength;
    this.columnType = builder.columnType;
    this.flags = builder.flags;
    this.decimals = builder.decimals;
  }

  public static Builder builder() {
    return new Builder();
  }

  public static class Builder {

    private int sequenceId;

    private String catalog = "def";

    private String schema = "";

    private String table = "";

    private String orgTable = "";

    private String name = "";

    private String orgName = "";

    private CharacterSet characterSet = CharacterSet.DEFAULT;

    private long columnLength = Integer.MAX_VALUE;

    private ColumnType columnType;

    private Set<ColumnFlag> flags = EnumSet.noneOf(ColumnFlag.class);

    private int decimals;

    private Builder() {}

    public Builder sequenceId(int sequenceId) {
      this.sequenceId = sequenceId;
      return this;
    }

    public Builder catalog(String catalog) {
      this.catalog = catalog;
      return this;
    }

    public Builder schema(String schema) {
      this.schema = schema;
      return this;
    }

    public Builder table(String table) {
      this.table = table;
      return this;
    }

    public Builder orgTable(String orgTable) {
      this.orgTable = orgTable;
      return this;
    }

    public Builder name(String name) {
      this.name = name;
      return this;
    }

    public Builder orgName(String orgName) {
      this.orgName = orgName;
      return this;
    }

    public Builder characterSet(CharacterSet characterSet) {
      this.characterSet = characterSet;
      return this;
    }

    public Builder columnLength(long columnLength) {
      this.columnLength = columnLength;
      return this;
    }

    public Builder columnType(ColumnType columnType) {
      this.columnType = columnType;
      return this;
    }

    public Builder addFlags(ColumnFlag... flags) {
      Collections.addAll(this.flags, flags);
      return this;
    }

    public Builder decimals(int decimals) {
      this.decimals = decimals;
      return this;
    }

    public ColumnDefinitionPacket build() {
      return new ColumnDefinitionPacket(this);
    }
  }
}
