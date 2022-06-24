package com.wojustme.goblin.server.mysql.decoder;

import com.wojustme.goblin.server.mysql.packet.command.CommandPacket;
import com.wojustme.goblin.server.mysql.packet.command.QueryCommandPacket;
import com.wojustme.goblin.server.mysql.protocol.CharacterSet;
import com.wojustme.goblin.server.mysql.protocol.Command;
import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.DecoderException;

import java.util.List;
import java.util.Optional;

public class CommandPacketDecoder extends AbstractPacketDecoder {
  @Override
  protected void decodePacket(
      ChannelHandlerContext ctx, int sequenceId, ByteBuf packet, List<Object> out) {
    final CharacterSet clientCharset = CharacterSet.getClientCharsetAttr(ctx.channel());

    final byte commandCode = packet.readByte();
    final Optional<Command> command = Command.findByCode(commandCode);
    if (!command.isPresent()) {
      throw new DecoderException("Unknown command " + commandCode);
    }
    switch (command.get()) {
      case COM_QUERY:
        out.add(new QueryCommandPacket(sequenceId, readFixedLengthString(packet, packet.readableBytes(), clientCharset.charset)));
        break;
      default:
        out.add(new CommandPacket(sequenceId, command.get()));
    }
  }
}