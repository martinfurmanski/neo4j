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
package org.neo4j.causalclustering.catchup;

import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;
import io.netty.util.ReferenceCountUtil;

import org.neo4j.logging.Log;
import org.neo4j.logging.LogProvider;

class RequestMessageTypeHandler extends ChannelInboundHandlerAdapter
{
    private final Log log;
    private final CoreServerProtocol protocol;

    RequestMessageTypeHandler( CoreServerProtocol protocol, LogProvider logProvider )
    {
        this.protocol = protocol;
        this.log = logProvider.getLog( getClass() );
    }

    @Override
    public void channelRead( ChannelHandlerContext ctx, Object msg ) throws Exception
    {
        if ( protocol.isExpecting( CoreServerProtocol.State.MESSAGE_TYPE ) )
        {
            RequestMessageType requestMessageType = RequestMessageType.from( ((ByteBuf) msg).readByte() );

            if ( requestMessageType.equals( RequestMessageType.TX_PULL_REQUEST ) )
            {
                protocol.expect( CoreServerProtocol.State.TX_PULL );
            }
            else if ( requestMessageType.equals( RequestMessageType.STORE ) )
            {
                protocol.expect( CoreServerProtocol.State.GET_STORE );
            }
            else if ( requestMessageType.equals( RequestMessageType.STORE_ID ) )
            {
                protocol.expect( CoreServerProtocol.State.GET_STORE_ID );
            }
            else if ( requestMessageType.equals( RequestMessageType.CORE_SNAPSHOT ) )
            {
                protocol.expect( CoreServerProtocol.State.GET_CORE_SNAPSHOT );
            }
            else if ( requestMessageType.equals( RequestMessageType.REGISTER_LOCK_SESSION ) )
            {
                protocol.expect( CoreServerProtocol.State.REGISTER_LOCK_SESSION );
            }
            else if ( requestMessageType.equals( RequestMessageType.END_LOCK_SESSION ) )
            {
                protocol.expect( CoreServerProtocol.State.END_LOCK_SESSION );
            }
            else if ( requestMessageType.equals( RequestMessageType.STOP_LOCK_SESSION ) )
            {
                protocol.expect( CoreServerProtocol.State.STOP_LOCK_SESSION );
            }
            else if ( requestMessageType.equals( RequestMessageType.ACQUIRE_LOCKS ) )
            {
                protocol.expect( CoreServerProtocol.State.ACQUIRE_LOCKS );
            }
            else if ( requestMessageType.equals( RequestMessageType.RELEASE_LOCK ) )
            {
                protocol.expect( CoreServerProtocol.State.RELEASE_LOCK );
            }
            else
            {
                log.warn( "No handler found for message type %s", requestMessageType );
            }

            ReferenceCountUtil.release( msg );
        }
        else
        {
            ctx.fireChannelRead( msg );
        }
    }
}
