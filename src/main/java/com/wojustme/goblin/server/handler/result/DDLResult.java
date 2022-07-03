package com.wojustme.goblin.server.handler.result;

public class DDLResult extends HandlerResult {

  private final int affect;

  public DDLResult(int affect) {
    this.affect = affect;
  }

  public int getAffect() {
    return affect;
  }
}
