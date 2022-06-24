package com.wojustme.goblin.server;

import com.wojustme.goblin.server.mysql.MysqlProxyServer;

public class GoblinServer {
  public static void main(String[] args) {
    new MysqlProxyServer(3310).start();
  }
}
