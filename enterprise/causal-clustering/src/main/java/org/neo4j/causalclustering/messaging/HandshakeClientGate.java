/*
 * Copyright (c) 2002-2018 "Neo Technology,"
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
package org.neo4j.causalclustering.messaging;

import io.netty.channel.Channel;
import io.netty.channel.ChannelDuplexHandler;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelPromise;

import java.util.concurrent.CompletableFuture;

import org.neo4j.causalclustering.protocol.handshake.ClientHandshakeException;
import org.neo4j.causalclustering.protocol.handshake.HandshakeClientInitializer;
import org.neo4j.causalclustering.protocol.handshake.HandshakeFinishedEvent;
import org.neo4j.causalclustering.protocol.handshake.ServerMessage;
import org.neo4j.logging.Log;

/**
 * Gates messages written before the handshake has completed. The handshake is finalized
 * by firing a HandshakeFinishedEvent (as a netty user event) in {@link HandshakeClientInitializer}.
 */
public class HandshakeClientGate extends ChannelDuplexHandler
{
    private final CompletableFuture<Void> handshakePromise = new CompletableFuture<>();
    private final Channel channel;
    private final Log log;

    public HandshakeClientGate( Channel channel, Log log )
    {
        this.channel = channel;
        this.log = log;
    }

    @Override
    public void userEventTriggered( ChannelHandlerContext ctx, Object evt ) throws Exception
    {
        if ( HandshakeFinishedEvent.getSuccess().equals( evt ) )
        {
            log.info( "Handshake gate success" );
            handshakePromise.complete( null );
        }
        else if ( HandshakeFinishedEvent.getFailure().equals( evt ) )
        {
            log.warn( "Handshake gate failed" );
            handshakePromise.completeExceptionally( new ClientHandshakeException( "Handshake failed" ) );
            channel.close();
        }
        else
        {
            super.userEventTriggered( ctx, evt );
        }
    }

    @Override
    public void write( ChannelHandlerContext ctx, Object msg, ChannelPromise promise ) throws Exception
    {
        if ( handshakePromise.isDone() || msg instanceof ServerMessage )
        {
            ctx.write( msg, promise );
        }
        else
        {
            handshakePromise.whenComplete( ( ignored, failure ) -> {
                if ( failure == null )
                {
                    channel.writeAndFlush( msg, promise );
                }
            } );
        }
    }
}
