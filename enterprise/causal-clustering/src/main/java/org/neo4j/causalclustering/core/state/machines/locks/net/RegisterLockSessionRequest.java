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
package org.neo4j.causalclustering.core.state.machines.locks.net;

import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.ByteToMessageDecoder;
import io.netty.handler.codec.MessageToByteEncoder;

import java.util.List;

import org.neo4j.causalclustering.catchup.RequestMessageType;
import org.neo4j.causalclustering.core.replication.session.GlobalSession;
import org.neo4j.causalclustering.messaging.CoreRequest;

public class RegisterLockSessionRequest implements CoreRequest
{
    private final GlobalSession session;

    RegisterLockSessionRequest( GlobalSession session )
    {
        this.session = session;
    }

    @Override
    public RequestMessageType messageType()
    {
        return RequestMessageType.REGISTER_LOCK_SESSION;
    }

    public GlobalSession session()
    {
        return session;
    }

    public static class Encoder extends MessageToByteEncoder<RegisterLockSessionRequest>
    {
        @Override
        protected void encode( ChannelHandlerContext ctx, RegisterLockSessionRequest msg, ByteBuf out ) throws Exception
        {
            GlobalSession.Marshal.INSTANCE.marshal( msg.session, out );
        }
    }

    public static class Decoder extends ByteToMessageDecoder
    {
        @Override
        protected void decode( ChannelHandlerContext ctx, ByteBuf in, List<Object> out ) throws Exception
        {
            GlobalSession session = GlobalSession.Marshal.INSTANCE.unmarshal( in );
            out.add( new RegisterLockSessionRequest( session ) );
        }
    }
}
