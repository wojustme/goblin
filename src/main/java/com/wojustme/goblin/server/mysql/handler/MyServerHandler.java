package com.wojustme.goblin.server.mysql.handler;

import com.wojustme.goblin.common.GoblinContext;
import com.wojustme.goblin.common.UserSession;
import com.wojustme.goblin.meta.catalog.model.DataType;
import com.wojustme.goblin.server.handler.SessionHandler;
import com.wojustme.goblin.server.handler.result.AffectSummaryResult;
import com.wojustme.goblin.server.handler.result.FailedResult;
import com.wojustme.goblin.server.handler.result.HandlerResult;
import com.wojustme.goblin.server.handler.result.SucceedResult;
import com.wojustme.goblin.server.mysql.common.ProtocolConstants;
import com.wojustme.goblin.server.mysql.decoder.AbstractPacketDecoder;
import com.wojustme.goblin.server.mysql.decoder.CommandPacketDecoder;
import com.wojustme.goblin.server.mysql.packet.ColumnCountPacket;
import com.wojustme.goblin.server.mysql.packet.ColumnDefinitionPacket;
import com.wojustme.goblin.server.mysql.packet.EofResponsePacket;
import com.wojustme.goblin.server.mysql.packet.ErrorResponsePacket;
import com.wojustme.goblin.server.mysql.packet.HandshakePacket;
import com.wojustme.goblin.server.mysql.packet.HandshakeResponsePacket;
import com.wojustme.goblin.server.mysql.packet.OkResponsePacket;
import com.wojustme.goblin.server.mysql.packet.ResultSetRowPacket;
import com.wojustme.goblin.server.mysql.packet.command.CommandPacket;
import com.wojustme.goblin.server.mysql.packet.command.QuitCommandPacket;
import com.wojustme.goblin.server.mysql.protocol.CapabilityFlags;
import com.wojustme.goblin.server.mysql.protocol.CharacterSet;
import com.wojustme.goblin.server.mysql.protocol.ColumnType;
import com.wojustme.goblin.server.mysql.protocol.ServerStatusFlags;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.tuple.Pair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.EnumSet;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.atomic.AtomicInteger;

public class MyServerHandler extends ChannelInboundHandlerAdapter {

  private static final AtomicInteger COUNT = new AtomicInteger(0);

  private static final Logger LOGGER = LoggerFactory.getLogger(MyServerHandler.class);
  private final byte[] salt = new byte[21];

  private final GoblinContext goblinContext;
  private final Map<String, SessionHandler> sessionHandlers;

  public MyServerHandler(GoblinContext goblinContext, Map<String, SessionHandler> sessionHandlers) {
    for (int i = 0; i < salt.length - 1; i++) {
      salt[i] = 1;
    }
    new Random().nextBytes(salt);
    salt[20] = 0;
    this.goblinContext = goblinContext;
    this.sessionHandlers = sessionHandlers;
  }

  @Override
  public void channelActive(ChannelHandlerContext ctx) throws Exception {
    LOGGER.info("ChannelId:{} active.", ctx.channel().id());
    final EnumSet<CapabilityFlags> capabilities = CapabilityFlags.getImplicitCapabilities();
    CapabilityFlags.setCapabilitiesAttr(ctx.channel(), capabilities);
    ctx.writeAndFlush(
        HandshakePacket.builder()
            .serverVersion("Goblin MySQL Proxy")
            .connectionId(COUNT.getAndIncrement())
            .addAuthData(salt)
            .characterSet(CharacterSet.UTF8_BIN)
            .addCapabilities(capabilities)
            .addServerStatus(ServerStatusFlags.SERVER_STATUS_AUTOCOMMIT)
            .authPluginName(ProtocolConstants.MYSQL_NATIVE_PASSWORD)
            .build());
  }

  @Override
  public void channelInactive(ChannelHandlerContext ctx) throws Exception {
    LOGGER.info("ChannelId:{} inactive.", ctx.channel().id());
    super.channelInactive(ctx);
  }

  @Override
  public void channelRead(ChannelHandlerContext ctx, Object msg) throws Exception {
    switch (msg) {
      case HandshakeResponsePacket packet -> handleHandshakeResponse(ctx, packet);
      case CommandPacket packet -> handleCommand(ctx, packet);
//      case UseDbCommandPacket packet -> handleUseDb(ctx, packet);
      default -> {
        LOGGER.error("Unknown packet type: {}", msg.getClass());
        throw new IllegalStateException("Unknown packet type: " + msg.getClass());
      }
    }
  }


  private String getSessionId(ChannelHandlerContext ctx) {
    return ctx.channel().id().asShortText();
  }

  private void handleHandshakeResponse(
      ChannelHandlerContext ctx, HandshakeResponsePacket response) {
    final String sessionId = getSessionId(ctx);
    final String username = response.username;
    // TODO check username and password

    final UserSession userSession = new UserSession(sessionId, username);
    if (StringUtils.isNotEmpty(response.database)) {
      userSession.setDatabase(response.database);
    }
    final SessionHandler handler = new SessionHandler(goblinContext, userSession);
    sessionHandlers.put(sessionId, handler);

    // remove auth connect decoder
    ctx.pipeline()
        .replace(AbstractPacketDecoder.class, "CommandPacketDecoder", new CommandPacketDecoder());
    ctx.writeAndFlush(
        OkResponsePacket.builder()
            .addStatusFlags(ServerStatusFlags.SERVER_STATUS_AUTOCOMMIT)
            .sequenceId(2)
            .build());
  }


  private void handleCommand(ChannelHandlerContext ctx, CommandPacket command) {
    final String sessionId = getSessionId(ctx);
    LOGGER.info("SessionId:{} receive sql: {}", sessionId, command);
    final SessionHandler sessionHandler = sessionHandlers.get(sessionId);
    if (command instanceof QuitCommandPacket quit) {
      sessionHandlers.remove(sessionId);
      int sequenceId = quit.sequenceId;
      ctx.writeAndFlush(OkResponsePacket.builder()
              .addStatusFlags(ServerStatusFlags.SERVER_SESSION_STATE_CHANGED)
              .sequenceId(++sequenceId)
              .build());
    } else {
      HandlerResult result = command.handle(sessionHandler);
      switch (result) {
        case SucceedResult succeedResult -> okFlush(ctx, command.sequenceId, succeedResult);
        case AffectSummaryResult affectSummaryResult -> ddlFlush(ctx, command.sequenceId, affectSummaryResult);
        case FailedResult failedResult -> errorFlush(ctx, command.sequenceId, failedResult);
        default -> throw new RuntimeException("Not support result type: " + result.getClass());
      }
    }
  }

  private void okFlush(ChannelHandlerContext ctx, int sequenceId, SucceedResult result) {

    // write field's count.
    ctx.write(new ColumnCountPacket(++sequenceId, result.getFieldCount()));

    // write field.
    for (Pair<String, DataType> fieldPair : result.getFields()) {
      ctx.write(ColumnDefinitionPacket.builder()
              .sequenceId(++sequenceId)
              .name(fieldPair.getLeft())
              .orgName(fieldPair.getLeft())
              .columnType(goblinTypeToMysqlType(fieldPair.getRight()))
              .build());
    }

    ctx.write(new EofResponsePacket(++sequenceId, 0));

    // write row data's list.
    for (List<Object> rowData : result.getRows()) {
      ctx.write(new ResultSetRowPacket(++sequenceId, rowData.stream().map(col->{
        if (col == null) {
          return "NULL";
        } else {
          return col.toString();
        }
      }).toList()));
    }

    ctx.writeAndFlush(new EofResponsePacket(++sequenceId, 0));
  }


  private void ddlFlush(ChannelHandlerContext ctx, int sequenceId, AffectSummaryResult affectSummaryResult) {
    ctx.writeAndFlush(
            OkResponsePacket.builder()
                    .addStatusFlags(ServerStatusFlags.SERVER_STATUS_AUTOCOMMIT)
                    .sequenceId(++sequenceId)
                    .affectedRows(affectSummaryResult.getAffect())
                    .build());
  }


  private void errorFlush(ChannelHandlerContext ctx, int sequenceId, FailedResult result) {
    final ErrorResponsePacket errorPacket = ErrorResponsePacket.createOf(++sequenceId, result.getErrorCode(), result.getMessage());
    ctx.writeAndFlush(errorPacket);
  }

  private ColumnType goblinTypeToMysqlType(DataType goblinType) {
   return switch (goblinType) {
      case STRING -> ColumnType.MYSQL_TYPE_STRING;
      default -> throw new RuntimeException("Goblin's type convert to mysql's type fail, type: " + goblinType);
    };
  }
}
