package com.wojustme.goblin.server.handler.result;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import com.wojustme.goblin.meta.catalog.model.DataType;
import org.apache.commons.lang3.tuple.Pair;

import java.util.ArrayList;
import java.util.List;

public class SucceedResult extends HandlerResult {

  private final List<Pair<String, DataType>> fields = new ArrayList<>();

  private final List<List<Object>> rows = new ArrayList<>();

  public SucceedResult() {}

  public List<Pair<String, DataType>> getFields() {
    return fields;
  }

  public List<List<Object>> getRows() {
    return rows;
  }

  public int getFieldCount() {
    return fields.size();
  }

  public SucceedResult addField(String name, DataType type) {
    fields.add(Pair.of(name, type));
    return this;
  }

  public SucceedResult addRow(Object... row) {
    Preconditions.checkArgument(row.length == getFieldCount(), "Illegal row data.");
    rows.add(ImmutableList.copyOf(row));
    return this;
  }
}
