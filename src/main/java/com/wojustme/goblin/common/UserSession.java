package com.wojustme.goblin.common;

public class UserSession {

  public final String sessionId;

  private final String username;

  public UserSession(String sessionId, String username) {
    this.sessionId = sessionId;
    this.username = username;
  }

  public void setDatabase(String database) {}
}
