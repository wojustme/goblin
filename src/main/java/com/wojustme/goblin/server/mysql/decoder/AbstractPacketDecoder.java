package com.wojustme.goblin.server.mysql.decoder;

import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.ByteToMessageDecoder;
import io.netty.handler.codec.CodecException;

import java.nio.charset.Charset;
import java.util.List;

public abstract class AbstractPacketDecoder extends ByteToMessageDecoder {
  @Override
  protected void decode(ChannelHandlerContext ctx, ByteBuf in, List<Object> out) throws Exception {
    if (in.isReadable(4)) {
      in.markReaderIndex();
      final int packetSize = in.readUnsignedMediumLE();
      final int sequenceId = in.readByte();
      if (!in.isReadable(packetSize)) {
        in.resetReaderIndex();
        return;
      }
      final ByteBuf packet = in.readSlice(packetSize);

      decodePacket(ctx, sequenceId, packet, out);
    }
  }

  protected abstract void decodePacket(
      ChannelHandlerContext ctx, int sequenceId, ByteBuf packet, List<Object> out);

  private static final int NULL_VALUE = 0xfb;
  private static final int SHORT_VALUE = 0xfc;
  private static final int MEDIUM_VALUE = 0xfd;
  private static final int LONG_VALUE = 0xfe;

  protected String readNullTerminatedString(ByteBuf buf, Charset charset) {
    final int len = findNullTermLen(buf);
    if (len < 0) {
      return null;
    }
    final String s = readFixedLengthString(buf, len, charset);
    buf.readByte();
    return s;
  }

  protected int findNullTermLen(ByteBuf buf) {
    final int termIdx = buf.indexOf(buf.readerIndex(), buf.capacity(), (byte) 0);
    if (termIdx < 0) {
      return -1;
    }
    return termIdx - buf.readerIndex();
  }

  protected String readFixedLengthString(ByteBuf buf, int length, Charset charset) {
    if (length < 0) {
      return null;
    }
    final String s = buf.toString(buf.readerIndex(), length, charset);
    buf.readerIndex(buf.readerIndex() + length);
    return s;
  }

  protected int readLengthEncodedInteger(ByteBuf buf) {
    return readLengthEncodedInteger(buf, buf.readUnsignedByte());
  }

  private int readLengthEncodedInteger(ByteBuf buf, int firstByte) {
    firstByte = firstByte & 0xff;
    if (firstByte < NULL_VALUE) {
      return firstByte;
    }
    if (firstByte == NULL_VALUE) {
      return -1;
    }
    if (firstByte == SHORT_VALUE) {
      return buf.readUnsignedShortLE();
    }
    if (firstByte == MEDIUM_VALUE) {
      return buf.readUnsignedMediumLE();
    }
    if (firstByte == LONG_VALUE) {
      final long length = buf.readLongLE();
      if (length < 0) {
        throw new CodecException(
            "Received a length value too large to handle: " + Long.toHexString(length));
      }
      return (int) length;
    }
    throw new CodecException("Received an invalid length value " + firstByte);
  }

  protected String readLengthEncodedString(ByteBuf buf, Charset charset) {
    final long len = readLengthEncodedInteger(buf);
    return readFixedLengthString(buf, (int) len, charset);
  }
}
