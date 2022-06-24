package com.wojustme.goblin.server.mysql.encoder;

import com.wojustme.goblin.server.mysql.common.ProtocolConstants;
import com.wojustme.goblin.server.mysql.packet.ColumnCountPacket;
import com.wojustme.goblin.server.mysql.packet.ColumnDefinitionPacket;
import com.wojustme.goblin.server.mysql.packet.EofResponsePacket;
import com.wojustme.goblin.server.mysql.packet.ErrorResponsePacket;
import com.wojustme.goblin.server.mysql.packet.HandshakePacket;
import com.wojustme.goblin.server.mysql.packet.MysqlPacket;
import com.wojustme.goblin.server.mysql.packet.OkResponsePacket;
import com.wojustme.goblin.server.mysql.packet.ResultSetRowPacket;
import com.wojustme.goblin.server.mysql.protocol.CapabilityFlags;
import com.wojustme.goblin.server.mysql.protocol.CharacterSet;
import com.wojustme.goblin.server.mysql.protocol.ColumnFlag;
import com.wojustme.goblin.server.mysql.protocol.ServerStatusFlags;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufUtil;
import io.netty.buffer.Unpooled;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.MessageToByteEncoder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.charset.Charset;
import java.util.EnumSet;

public class ServerPacketEncoder extends MessageToByteEncoder<MysqlPacket> {

  private static final Logger LOGGER = LoggerFactory.getLogger(ServerPacketEncoder.class);

  @Override
  protected void encode(ChannelHandlerContext ctx, MysqlPacket packet, ByteBuf buf)
      throws Exception {
    final int writerIdx = buf.writerIndex();
    buf.writeInt(0); // Advance the writer index so we can set the packet length after encoding
    encodePacket(ctx, packet, buf);
    final int len = buf.writerIndex() - writerIdx - 4;
    buf.setMediumLE(writerIdx, len).setByte(writerIdx + 3, packet.sequenceId);
  }

  private void encodePacket(ChannelHandlerContext ctx, MysqlPacket packet, ByteBuf buf) {
    final EnumSet<CapabilityFlags> capabilities =
        CapabilityFlags.getCapabilitiesAttr(ctx.channel());
    final Charset serverCharset = CharacterSet.getServerCharsetAttr(ctx.channel()).charset;

    switch (packet) {
      case HandshakePacket p  -> encodeHandshake(p, buf);
      case EofResponsePacket p  -> encodeEofResponse(capabilities, p, buf);
      case OkResponsePacket p  -> encodeOkResponse(capabilities, serverCharset, p, buf);
      case ErrorResponsePacket p  -> encodeErrorResponse(capabilities, p, buf);
      case ColumnCountPacket p  -> writeLengthEncodedInt(buf, (long) p.fieldCount);
      case ColumnDefinitionPacket p  -> encodeColumnDefinition(serverCharset, p, buf);
      case ResultSetRowPacket rowPacket  -> {
        for (String value : rowPacket.values) {
          writeLengthEncodedString(buf, value, serverCharset);
        }
      }
      default -> {
        LOGGER.error("Unknown packet type: {}", packet.getClass());
        throw new IllegalStateException("Unknown packet type: " + packet.getClass());
      }
    }
  }

  private void encodeErrorResponse(
      EnumSet<CapabilityFlags> capabilities, ErrorResponsePacket response, ByteBuf buf) {
    buf.writeByte(0xff);
    buf.writeShortLE(response.errorNumber);
    if (capabilities.contains(CapabilityFlags.CLIENT_PROTOCOL_41)) {
      buf.writeByte(0x23).writeBytes(response.sqlState);
    }
    buf.writeBytes(response.message.getBytes());
  }

  private void encodeOkResponse(
      EnumSet<CapabilityFlags> capabilities,
      Charset serverCharset,
      OkResponsePacket response,
      ByteBuf buf) {
    buf.writeByte(0);
    writeLengthEncodedInt(buf, response.affectedRows);
    writeLengthEncodedInt(buf, response.lastInsertId);
    if (capabilities.contains(CapabilityFlags.CLIENT_PROTOCOL_41)) {
      buf.writeShortLE(ServerStatusFlags.encode(response.statusFlags))
          .writeShortLE(response.warnings);

    } else if (capabilities.contains(CapabilityFlags.CLIENT_TRANSACTIONS)) {
      buf.writeShortLE(ServerStatusFlags.encode(response.statusFlags));
    }
    if (capabilities.contains(CapabilityFlags.CLIENT_SESSION_TRACK)) {
      writeLengthEncodedString(buf, response.info, serverCharset);
      if (response.statusFlags.contains(ServerStatusFlags.SERVER_SESSION_STATE_CHANGED)) {
        writeLengthEncodedString(buf, response.sessionStateChanges, serverCharset);
      }
    } else {
      if (response.info != null) {
        buf.writeCharSequence(response.info, serverCharset);
      }
    }
  }

  private void encodeEofResponse(
      EnumSet<CapabilityFlags> capabilities, EofResponsePacket eof, ByteBuf buf) {
    buf.writeByte(0xfe);
    if (capabilities.contains(CapabilityFlags.CLIENT_PROTOCOL_41)) {
      buf.writeShortLE(eof.warnings).writeShortLE(ServerStatusFlags.encode(eof.statusFlags));
    }
  }

  private void encodeHandshake(HandshakePacket handshake, ByteBuf buf) {
    buf.writeByte(handshake.protocolVersion)
        .writeBytes(handshake.serverVersion.array())
        .writeByte(ProtocolConstants.NUL_BYTE)
        .writeIntLE(handshake.connectionId)
        .writeBytes(handshake.authPluginData, ProtocolConstants.AUTH_PLUGIN_DATA_PART1_LEN)
        .writeByte(ProtocolConstants.NUL_BYTE)
        .writeShortLE(CapabilityFlags.encode(handshake.capabilities))
        .writeByte(handshake.characterSet.id)
        .writeShortLE(ServerStatusFlags.encode(handshake.serverStatus))
        .writeShortLE(CapabilityFlags.encode(handshake.capabilities) >> Short.SIZE);
    if (handshake.capabilities.contains(CapabilityFlags.CLIENT_PLUGIN_AUTH)) {
      buf.writeByte(
          handshake.authPluginData.readableBytes() + ProtocolConstants.AUTH_PLUGIN_DATA_PART1_LEN);
    } else {
      buf.writeByte(ProtocolConstants.NUL_BYTE);
    }
    buf.writeZero(ProtocolConstants.HANDSHAKE_RESERVED_BYTES);
    if (handshake.capabilities.contains(CapabilityFlags.CLIENT_SECURE_CONNECTION)) {
      final int padding =
          ProtocolConstants.AUTH_PLUGIN_DATA_PART2_MIN_LEN
              - handshake.authPluginData.readableBytes();
      buf.writeBytes(handshake.authPluginData);
      if (padding > 0) {
        buf.writeZero(padding);
      }
    }
    if (handshake.capabilities.contains(CapabilityFlags.CLIENT_PLUGIN_AUTH)) {
      ByteBufUtil.writeUtf8(buf, handshake.authPluginName);
      buf.writeByte(ProtocolConstants.NUL_BYTE);
    }
  }

  protected void encodeColumnDefinition(Charset serverCharset, ColumnDefinitionPacket packet, ByteBuf buf) {
    writeLengthEncodedString(buf, packet.catalog, serverCharset);
    writeLengthEncodedString(buf, packet.schema, serverCharset);
    writeLengthEncodedString(buf, packet.table, serverCharset);
   writeLengthEncodedString(buf, packet.orgTable, serverCharset);
   writeLengthEncodedString(buf, packet.name, serverCharset);
   writeLengthEncodedString(buf, packet.orgName, serverCharset);
    buf.writeByte(0x0c);
    buf.writeShortLE(packet.characterSet.id)
            .writeIntLE((int) packet.columnLength)
            .writeByte(packet.columnType.value)
            .writeShortLE(ColumnFlag.encode(packet.flags))
            .writeByte(packet.decimals)
            .writeShort(0);
  }

  private static final int NULL_VALUE = 0xfb;
  private static final int SHORT_VALUE = 0xfc;
  private static final int MEDIUM_VALUE = 0xfd;
  private static final int LONG_VALUE = 0xfe;

  public static void writeLengthEncodedInt(ByteBuf buf, Long n) {
    if (n == null) {
      buf.writeByte(NULL_VALUE);
    } else if (n < 0) {
      throw new IllegalArgumentException("Cannot encode a negative length: " + n);
    } else if (n < NULL_VALUE) {
      buf.writeByte(n.intValue());
    } else if (n < 0xffff) {
      buf.writeByte(SHORT_VALUE);
      buf.writeShortLE(n.intValue());
    } else if (n < 0xffffff) {
      buf.writeByte(MEDIUM_VALUE);
      buf.writeMediumLE(n.intValue());
    } else {
      buf.writeByte(LONG_VALUE);
      buf.writeLongLE(n);
    }
  }

  public static void writeLengthEncodedString(ByteBuf buf, CharSequence sequence, Charset charset) {
    final ByteBuf tmpBuf = Unpooled.buffer();
    try {
      tmpBuf.writeCharSequence(sequence, charset);
      writeLengthEncodedInt(buf, (long) tmpBuf.readableBytes());
      buf.writeBytes(tmpBuf);
    } finally {
      tmpBuf.release();
    }
  }
}
