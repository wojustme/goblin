package com.wojustme.goblin.server.mysql.protocol;

import java.util.Set;

/** <a href="https://dev.mysql.com/doc/internals/en/status-flags.html">status-flags</a> */
public enum ServerStatusFlags {
  SERVER_STATUS_IN_TRANS(0x0001),
  SERVER_STATUS_AUTOCOMMIT(0x0002),
  SERVER_MORE_RESULTS_EXISTS(0x0008),
  SERVER_STATUS_NO_GOOD_INDEX_USED(0x0010),
  SERVER_STATUS_NO_INDEX_USED(0x0020),
  SERVER_STATUS_CURSOR_EXISTS(0x0040),
  SERVER_STATUS_LAST_ROW_SENT(0x0080),
  SERVER_STATUS_DB_DROPPED(0x0100),
  SERVER_STATUS_NO_BACKSLASH_ESCAPES(0x0200),
  SERVER_STATUS_METADATA_CHANGED(0x0400),
  SERVER_QUERY_WAS_SLOW(0x0800),
  SERVER_PS_OUT_PARAMS(0x1000),
  SERVER_STATUS_IN_TRANS_READONLY(0x2000),
  SERVER_SESSION_STATE_CHANGED(0x4000),
  ;
  public final int value;

  ServerStatusFlags(int value) {
    this.value = value;
  }

  public static int encode(Set<ServerStatusFlags> serverStatus) {
    int vector = 0;
    for (ServerStatusFlags elem : serverStatus) {
      vector |= elem.value;
    }
    return vector;
  }
}
