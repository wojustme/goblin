package com.wojustme.goblin.server.mysql.protocol;

import io.netty.channel.Channel;
import io.netty.util.AttributeKey;

import java.nio.charset.Charset;
import java.nio.charset.UnsupportedCharsetException;

public enum CharacterSet {
  UTF8_GENERAL_CI(33, "UTF-8"),
  UTF8_BIN(83, "UTF-8"),
  ;
  public final int id;
  public final Charset charset;

  CharacterSet(int id, String charsetName) {
    this.id = id;
    try {
      this.charset = Charset.forName(charsetName);
    } catch (UnsupportedCharsetException e) {
      throw new RuntimeException("Not support charset name: " + charsetName);
    }
  }

  public static final CharacterSet DEFAULT = UTF8_GENERAL_CI;

  public static CharacterSet findById(int id) {
    for (CharacterSet elem : values()) {
      if (elem.id == id) {
        return elem;
      }
    }
    throw new RuntimeException("Not found charset by id: " + id);
  }

  private static final AttributeKey<CharacterSet> SERVER_CHARSET_KEY =
      AttributeKey.newInstance(CharacterSet.class.getName() + "-server");
  private static final AttributeKey<CharacterSet> CLIENT_CHARSET_KEY =
      AttributeKey.newInstance(CharacterSet.class.getName() + "-client");

  public static CharacterSet getServerCharsetAttr(Channel channel) {
    return getCharSetAttr(SERVER_CHARSET_KEY, channel);
  }

  public static CharacterSet getClientCharsetAttr(Channel channel) {
    return getCharSetAttr(CLIENT_CHARSET_KEY, channel);
  }

  private static CharacterSet getCharSetAttr(AttributeKey<CharacterSet> key, Channel channel) {
    if (channel.hasAttr(key)) {
      return channel.attr(key).get();
    }
    return DEFAULT;
  }
}
