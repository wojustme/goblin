package com.wojustme.goblin.server.mysql;

import com.wojustme.goblin.common.GoblinContext;
import com.wojustme.goblin.server.handler.SessionHandler;
import com.wojustme.goblin.server.mysql.decoder.ClientConnectionPacketDecoder;
import com.wojustme.goblin.server.mysql.decoder.CommandPacketDecoder;
import com.wojustme.goblin.server.mysql.encoder.ServerPacketEncoder;
import com.wojustme.goblin.server.mysql.handler.MyServerHandler;
import io.netty.bootstrap.ServerBootstrap;
import io.netty.channel.Channel;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.ChannelOption;
import io.netty.channel.ChannelPipeline;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.nio.NioServerSocketChannel;
import io.netty.channel.socket.nio.NioSocketChannel;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public class MysqlProxy implements AutoCloseable {

  private static final Logger LOGGER = LoggerFactory.getLogger(MysqlProxy.class);

  private final int port;

  private Channel channel;
  private EventLoopGroup bossGroup;
  private EventLoopGroup workerGroup;

  private final Map<String, SessionHandler> sessionHandlers = new ConcurrentHashMap<>();

  public MysqlProxy(int port) {
    this.port = port;
  }

  public void start(GoblinContext goblinContext) {
    final int workerTheadNum = Runtime.getRuntime().availableProcessors() * 2;
    bossGroup = new NioEventLoopGroup();
    workerGroup = new NioEventLoopGroup(workerTheadNum);
    final ChannelFuture channelFuture =
        new ServerBootstrap()
            .childOption(ChannelOption.SO_KEEPALIVE, true)
            .option(ChannelOption.SO_BACKLOG, 1024)
            .group(bossGroup, workerGroup)
            .channel(NioServerSocketChannel.class)
            .childHandler(
                new ChannelInitializer<NioSocketChannel>() {
                  @Override
                  protected void initChannel(NioSocketChannel ch) throws Exception {
                    final ChannelPipeline pipeline = ch.pipeline();
                    pipeline.addLast(new ServerPacketEncoder());
                    pipeline.addLast(new ClientConnectionPacketDecoder());
                    pipeline.addLast(new CommandPacketDecoder());
                    pipeline.addLast(new MyServerHandler(goblinContext, sessionHandlers));
                  }
                })
            .bind(port)
            .awaitUninterruptibly();
    channel = channelFuture.channel();
    LOGGER.info("MySQL Proxy Server start at port: {}.", port);
  }

  @Override
  public void close() throws Exception {
    LOGGER.info("MySQL Proxy Server shutdown at port: {}.", port);
    channel.close();
    workerGroup.shutdownGracefully().awaitUninterruptibly();
    bossGroup.shutdownGracefully().awaitUninterruptibly();
  }
}
