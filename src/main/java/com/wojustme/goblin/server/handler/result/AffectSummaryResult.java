package com.wojustme.goblin.server.handler.result;

public class AffectSummaryResult extends HandlerResult {

  private final int affect;

  public AffectSummaryResult(int affect) {
    this.affect = affect;
  }

  public int getAffect() {
    return affect;
  }
}
