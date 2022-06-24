package com.wojustme.goblin.server.mysql.protocol;

import java.util.Set;

public enum ColumnFlag {
  NOT_NULL(1),
  ;

  public final int value;
  ColumnFlag(int value) {
    this.value = value;
  }

  public static int encode(Set<ColumnFlag> flags) {
    int vector = 0;
    for (ColumnFlag elem : flags) {
      vector |= elem.value;
    }
    return vector;
  }
}
