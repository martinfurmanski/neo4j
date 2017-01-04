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
import org.neo4j.causalclustering.core.state.machines.locks.LockMode;
import org.neo4j.causalclustering.messaging.CoreRequest;
import org.neo4j.kernel.impl.locking.ResourceTypes;

public class ReleaseLockRequest implements CoreRequest
{
    private final GlobalSession session;
    private final LockMode mode;
    private final int typeId;
    private final long resourceId;

    ReleaseLockRequest( GlobalSession session, LockMode mode, int typeId, long resourceId )
    {
        this.session = session;
        this.mode = mode;
        this.typeId = typeId;
        this.resourceId = resourceId;
    }

    @Override
    public RequestMessageType messageType()
    {
        return RequestMessageType.RELEASE_LOCK;
    }

    @Override
    public String toString()
    {
        return "ReleaseLockRequest{" +
               "session=" + session +
               ", mode=" + mode +
               ", typeId=" + typeId +
               ", resourceId=" + resourceId +
               '}';
    }

    public GlobalSession session()
    {
        return session;
    }

    public LockMode mode()
    {
        return mode;
    }

    public int typeId()
    {
        return typeId;
    }

    public long resourceId()
    {
        return resourceId;
    }

    public static class Encoder extends MessageToByteEncoder<ReleaseLockRequest>
    {
        @Override
        protected void encode( ChannelHandlerContext ctx, ReleaseLockRequest msg, ByteBuf out ) throws Exception
        {
            GlobalSession.Marshal.INSTANCE.marshal( msg.session, out );

            out.writeInt( msg.mode.ordinal() );
            out.writeInt( msg.typeId );
            out.writeLong( msg.resourceId );
        }
    }

    public static class Decoder extends ByteToMessageDecoder
    {
        @Override
        protected void decode( ChannelHandlerContext ctx, ByteBuf in, List<Object> out ) throws Exception
        {
            GlobalSession session = GlobalSession.Marshal.INSTANCE.unmarshal( in );

            LockMode mode = LockMode.values()[in.readInt()];
            int typeId = in.readInt();
            long resourceId = in.readLong();

            out.add( new ReleaseLockRequest( session, mode, typeId, resourceId ) );
        }

    }
}
