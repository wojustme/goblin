package com.wojustme.goblin.server.mysql.decoder;

import com.wojustme.goblin.server.mysql.common.ProtocolConstants;
import com.wojustme.goblin.server.mysql.packet.HandshakeResponsePacket;
import com.wojustme.goblin.server.mysql.protocol.CapabilityFlags;
import com.wojustme.goblin.server.mysql.protocol.CharacterSet;
import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.DecoderException;

import java.util.EnumSet;
import java.util.List;

public class ClientConnectionPacketDecoder extends AbstractPacketDecoder {
  @Override
  protected void decodePacket(
      ChannelHandlerContext ctx, int sequenceId, ByteBuf packet, List<Object> out) {
    final EnumSet<CapabilityFlags> clientCapabilities =
        CapabilityFlags.decode((int) packet.readUnsignedIntLE());

    if (!clientCapabilities.contains(CapabilityFlags.CLIENT_PROTOCOL_41)) {
      throw new DecoderException("MySQL client protocol 4.1 support required");
    }

    final HandshakeResponsePacket.Builder builder = HandshakeResponsePacket.builder();

    builder.addCapabilities(clientCapabilities);
    builder.maxPacketSize((int) packet.readUnsignedIntLE());
    final CharacterSet characterSet = CharacterSet.findById(packet.readByte());
    builder.characterSet(characterSet);
    builder.authPluginName(ProtocolConstants.MYSQL_NATIVE_PASSWORD);
    packet.skipBytes(23); // skip unused bytes

    if (packet.isReadable()) {
      builder.username(readNullTerminatedString(packet, characterSet.charset));

      final EnumSet<CapabilityFlags> serverCapabilities =
          CapabilityFlags.getCapabilitiesAttr(ctx.channel());
      final EnumSet<CapabilityFlags> capabilities = EnumSet.copyOf(clientCapabilities);
      capabilities.retainAll(serverCapabilities);

      final int authResponseLength;
      if (capabilities.contains(CapabilityFlags.CLIENT_PLUGIN_AUTH_LENENC_CLIENT_DATA)) {
        authResponseLength = readLengthEncodedInteger(packet);
      } else if (capabilities.contains(CapabilityFlags.CLIENT_SECURE_CONNECTION)) {
        authResponseLength = packet.readUnsignedByte();
      } else {
        authResponseLength = findNullTermLen(packet);
      }
      builder.addAuthData(packet, authResponseLength);

      if (capabilities.contains(CapabilityFlags.CLIENT_CONNECT_WITH_DB)) {
        builder.database(readNullTerminatedString(packet, characterSet.charset));
      }

      if (capabilities.contains(CapabilityFlags.CLIENT_PLUGIN_AUTH)) {
        builder.authPluginName(readNullTerminatedString(packet, characterSet.charset));
      }

//      if (capabilities.contains(CapabilityFlags.CLIENT_CONNECT_ATTRS)) {
//        final long keyValueLen = readLengthEncodedInteger(packet);
//        for (int i = 0; i < keyValueLen; ) {
//          String key = readLengthEncodedString(packet, characterSet.charset);
//          String value = readLengthEncodedString(packet, characterSet.charset);
//          int keyLen = key.length();
//          if (StringUtils.isEmpty(key)) {
//            keyLen = 1;
//          }
//
//          int valLen = key.length();
//          if (StringUtils.isEmpty(value)) {
//            valLen = 1;
//          }
//          i += keyLen + valLen;
//          builder.addAttribute(key, value);
//        }
//      }
    }
    out.add(builder.build());
  }
}
