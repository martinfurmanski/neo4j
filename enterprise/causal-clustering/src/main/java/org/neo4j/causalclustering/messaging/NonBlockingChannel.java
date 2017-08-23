/*
 * Copyright (c) 2002-2017 "Neo Technology,"
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

import io.netty.bootstrap.Bootstrap;
import io.netty.channel.Channel;
import io.netty.channel.ChannelFuture;
import io.netty.util.concurrent.Future;

import org.neo4j.helpers.AdvertisedSocketAddress;
import org.neo4j.logging.Log;

import static java.util.concurrent.TimeUnit.MILLISECONDS;

class NonBlockingChannel
{
    private static final int CONNECT_BACKOFF_IN_MS = 250;

    private final Log log;
    private final Bootstrap bootstrap;
    private final AdvertisedSocketAddress destination;

    private volatile Channel nettyChannel;
    private volatile boolean disposed;

    NonBlockingChannel( Bootstrap bootstrap, final AdvertisedSocketAddress destination, final Log log )
    {
        this.bootstrap = bootstrap;
        this.destination = destination;
        this.log = log;

        doConnect();
    }

    private void doConnect()
    {
        if ( disposed )
        {
            return;
        }

        ChannelFuture cf = bootstrap.connect( destination.socketAddress() );
        cf.addListener( ( ChannelFuture f ) ->
        {
            if ( !f.isSuccess() )
            {
                f.channel().eventLoop().schedule( this::doConnect, CONNECT_BACKOFF_IN_MS, MILLISECONDS );
            }
            else
            {
                log.info( "Connected: " + f.channel() );
                f.channel().closeFuture().addListener( closed ->
                {
                    log.warn( String.format( "Lost connection to: %s (%s)", destination, nettyChannel.remoteAddress() ) );
                    f.channel().eventLoop().schedule( this::doConnect, CONNECT_BACKOFF_IN_MS, MILLISECONDS );
                } );
            }
        } );

        nettyChannel = cf.channel();

        /* this wait is purely optimistic to avoid getting early messages discarded and
        clients will have to rely on resending where the connection was not yet up */
        cf.awaitUninterruptibly( 1000, MILLISECONDS );
    }

    public void dispose()
    {
        disposed = true;
        nettyChannel.close();
    }

    public Future<Void> send( Object msg )
    {
        if ( disposed )
        {
            throw new IllegalStateException( "sending on disposed channel" );
        }

        try
        {
            return nettyChannel.writeAndFlush( msg );
        }
        catch ( Throwable e )
        {
            return nettyChannel.newFailedFuture( e );
        }
    }
}
