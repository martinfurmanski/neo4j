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
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.nio.NioDatagramChannel;

import java.util.concurrent.TimeUnit;

import org.neo4j.coreedge.server.AdvertisedSocketAddress;
import org.neo4j.coreedge.server.logging.ExceptionLoggingHandler;
import org.neo4j.helpers.NamedThreadFactory;
import org.neo4j.logging.Log;
import org.neo4j.logging.LogProvider;

public class SimpleDiscoveryClient
{
    public static final int RECONNECT_TIME_MS = 1_000;

    private final AdvertisedSocketAddress address;
    private final Log log;

    private final NioEventLoopGroup eventLoopGroup = new NioEventLoopGroup( 1, NamedThreadFactory.named( "discovery-client" ) );
    private final Bootstrap bootstrap;

    private Channel channel;
    private DiscoveryGossip firstGossip;

    private boolean destroyed = false;

    public SimpleDiscoveryClient( AdvertisedSocketAddress address, LogProvider logProvider, DiscoveryGossip firstGossip )
    {
        this.address = address;
        this.log = logProvider.getLog( getClass() );

        this.bootstrap = new Bootstrap()
                .group( eventLoopGroup )
                .channel( NioDatagramChannel.class )
                .handler( new ChannelInitializer<NioDatagramChannel>()
                {
                    @Override
                    protected void initChannel( NioDatagramChannel ch ) throws Exception
                    {
                        ch.pipeline().addLast( "gossip-encoder", new GossipEncoder() );
                        ch.pipeline().addLast( new ExceptionLoggingHandler( log ) );
                    }
                } );

        this.firstGossip = firstGossip;
        scheduleConnectionAttempt( 0 );
    }

    private void scheduleConnectionAttempt( long delayMillis )
    {
        eventLoopGroup.schedule( (Runnable) this::connect, delayMillis, TimeUnit.MILLISECONDS );
    }

    private synchronized void connect()
    {
        log.info( "Attempting connection to: " + address );

        ChannelFuture channelFuture = bootstrap.connect( address.socketAddress() );
        channelFuture.addListener( future -> {
            if ( future.isSuccess() )
            {
                onConnect( channelFuture.channel() );
            }
            else
            {
                scheduleConnectionAttempt( RECONNECT_TIME_MS );
            }
        } );
    }

    private synchronized void onConnect( Channel channel )
    {
        if( destroyed )
        {
            log.info( "Closing channel to already destroyed client: " + address );
            channel.close();
            return;
        }

        log.info( "Connected to: " + address );

        this.channel = channel;
        channel.closeFuture().addListener( future -> onClose( channel ) );

        if ( firstGossip != null )
        {
            write( firstGossip );
            firstGossip = null;
        }
    }

    private synchronized void onClose( Channel channel )
    {
        if( destroyed )
            return;

        if ( channel.equals( this.channel ) )
        {
            log.warn( "Spurious disconnect of: " + address );
            scheduleConnectionAttempt( RECONNECT_TIME_MS );
        }
    }

    /**
     * Sends the gossip to the other side if the connection is live. If the client is not
     * yet connected then it will track the latest gossip written and send it as soon as
     * the connection is established.
     */
    public synchronized void write( DiscoveryGossip gossip )
    {
        if( destroyed )
            throw new IllegalStateException( "You shall not use a destroyed client" );

        if ( channel != null && channel.isOpen() )
        {
            channel.writeAndFlush( gossip );
        }
        else
        {
            firstGossip = gossip;
        }
    }

    /**
     * The client must be destroyed when it will no longer be used.
     */
    public synchronized void destroy()
    {
        destroyed = true;
        eventLoopGroup.shutdownGracefully( 0, 0, TimeUnit.MILLISECONDS );

        if ( channel != null )
        {
            log.warn( "Disconnected: " + channel );
            channel.close();
            channel = null;
        }
    }
}
