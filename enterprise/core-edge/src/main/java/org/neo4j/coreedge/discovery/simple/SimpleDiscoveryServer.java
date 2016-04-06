/*
 * Copyright (c) 2002-2015 "Neo Technology,"
 * Network Engine for Objects in Lund AB [http://neotechnology.com]
 *
 * This file is part of Neo4j.
 *
 * Neo4j is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as
 * published by the Free Software Foundation, either version 3 of the
 * License, or (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program. If not, see <http://www.gnu.org/licenses/>.
 */
package org.neo4j.coreedge.discovery.simple;

import io.netty.bootstrap.Bootstrap;
import io.netty.channel.Channel;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.ChannelOption;
import io.netty.channel.ChannelPipeline;
import io.netty.channel.SimpleChannelInboundHandler;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.DatagramPacket;
import io.netty.channel.socket.nio.NioDatagramChannel;

import org.neo4j.coreedge.server.ListenSocketAddress;
import org.neo4j.coreedge.server.logging.ExceptionLoggingHandler;
import org.neo4j.helpers.NamedThreadFactory;
import org.neo4j.kernel.lifecycle.LifecycleAdapter;
import org.neo4j.logging.Log;
import org.neo4j.logging.LogProvider;

public class SimpleDiscoveryServer extends LifecycleAdapter
{
    private final ListenSocketAddress listenAddress;
    private final SimpleDiscoveryService discoveryService;
    private Channel listenChannel;

    private NioEventLoopGroup workerGroup;

    private final Log log;

    public SimpleDiscoveryServer( ListenSocketAddress listenAddress, SimpleDiscoveryService discoveryService,
            LogProvider logProvider )
    {
        this.discoveryService = discoveryService;
        this.listenAddress = listenAddress;
        this.log = logProvider.getLog( getClass() );
    }

    private class GossipDecoder extends SimpleChannelInboundHandler<DatagramPacket>
    {
        @Override
        protected void channelRead0( ChannelHandlerContext ctx, DatagramPacket msg ) throws Exception
        {
            DiscoveryGossip gossip = DiscoveryGossip.deserialize( msg.content() );
            ctx.fireChannelRead( gossip );
        }
    }

    private class GossipHandler extends SimpleChannelInboundHandler<DiscoveryGossip>
    {
        private final SimpleDiscoveryService discoveryService;

        private GossipHandler( SimpleDiscoveryService discoveryService )
        {
            this.discoveryService = discoveryService;
        }

        @Override
        protected void channelRead0( ChannelHandlerContext channelHandlerContext, DiscoveryGossip gossip ) throws Exception
        {
            discoveryService.processGossip( gossip );
        }
    }

    @Override
    public void start() throws Throwable
    {
        workerGroup = new NioEventLoopGroup( 1, NamedThreadFactory.named( "discovery-server" ) );

        log.info( "Starting server at: " + listenAddress );

        Bootstrap bootstrap = new Bootstrap()
                .group( workerGroup )
                .channel( NioDatagramChannel.class )
                .option( ChannelOption.SO_REUSEADDR, true )
                .localAddress( listenAddress.socketAddress() );

        bootstrap.handler( new ChannelInitializer<NioDatagramChannel>()
        {
            @Override
            protected void initChannel( NioDatagramChannel ch ) throws Exception
            {
                ChannelPipeline pipeline = ch.pipeline();
                pipeline.addLast( new GossipDecoder() );
                pipeline.addLast( new GossipHandler( discoveryService ) );
                pipeline.addLast( new ExceptionLoggingHandler( log ) );
            }
        } );

        listenChannel = bootstrap.bind().syncUninterruptibly().channel();
    }

    @Override
    public void stop() throws Throwable
    {
        listenChannel.close().sync();
        workerGroup.shutdownGracefully().sync();
    }
}
