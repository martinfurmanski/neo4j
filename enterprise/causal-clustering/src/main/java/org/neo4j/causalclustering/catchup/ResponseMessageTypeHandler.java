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

import static org.neo4j.causalclustering.catchup.ResponseMessageType.from;

public class ResponseMessageTypeHandler extends ChannelInboundHandlerAdapter
{
    private final Log log;
    private final CoreClientProtocol protocol;

    ResponseMessageTypeHandler( CoreClientProtocol protocol, LogProvider logProvider )
    {
        this.protocol = protocol;
        this.log = logProvider.getLog( getClass() );
    }

    @Override
    public void channelRead( ChannelHandlerContext ctx, Object msg ) throws Exception
    {
        if ( protocol.isExpecting( CoreClientProtocol.State.MESSAGE_TYPE ) )
        {
            ResponseMessageType responseMessageType = from( ((ByteBuf) msg).readByte() );

            switch ( responseMessageType )
            {
                case STORE_ID:
                    protocol.expect( CoreClientProtocol.State.STORE_ID );
                    break;
                case TX:
                    protocol.expect( CoreClientProtocol.State.TX_PULL_RESPONSE );
                    break;
                case FILE:
                    protocol.expect( CoreClientProtocol.State.FILE_HEADER );
                    break;
                case STORE_COPY_FINISHED:
                    protocol.expect( CoreClientProtocol.State.STORE_COPY_FINISHED );
                    break;
                case CORE_SNAPSHOT:
                    protocol.expect( CoreClientProtocol.State.CORE_SNAPSHOT );
                    break;
                case TX_STREAM_FINISHED:
                    protocol.expect( CoreClientProtocol.State.TX_STREAM_FINISHED );
                    break;
                case LOCK_RESPONSE:
                    protocol.expect( CoreClientProtocol.State.LOCK_RESPONSE );
                    break;
                default:
                    log.warn( "No handler found for message type %s", responseMessageType );
            }

            ReferenceCountUtil.release( msg );
        }
        else
        {
            ctx.fireChannelRead( msg );
        }
    }
}
