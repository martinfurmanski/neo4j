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

import java.util.Arrays;
import java.util.List;

import org.neo4j.causalclustering.catchup.RequestMessageType;
import org.neo4j.causalclustering.core.replication.session.GlobalSession;
import org.neo4j.causalclustering.core.state.machines.locks.LockMode;
import org.neo4j.causalclustering.messaging.CoreRequest;

public class AcquireLocksRequest implements CoreRequest
{
    private final GlobalSession session;
    private final LockMode mode;
    private final boolean blocking;
    private final int typeId;
    private final long[] resourceIds;

    AcquireLocksRequest( GlobalSession session, LockMode mode, boolean blocking, int typeId, long[] resourceIds )
    {
        this.session = session;
        this.mode = mode;
        this.blocking = blocking;
        this.typeId = typeId;
        this.resourceIds = resourceIds;
    }

    @Override
    public RequestMessageType messageType()
    {
        return RequestMessageType.ACQUIRE_LOCKS;
    }

    @Override
    public String toString()
    {
        return "AcquireLocksRequest{" +
               "session=" + session +
               ", mode=" + mode +
               ", blocking=" + blocking +
               ", typeId=" + typeId +
               ", resourceIds=" + Arrays.toString( resourceIds ) +
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

    public boolean blocking()
    {
        return blocking;
    }

    public int typeId()
    {
        return typeId;
    }

    public long[] resourceIds()
    {
        return resourceIds;
    }

    public static class Encoder extends MessageToByteEncoder<AcquireLocksRequest>
    {
        public void encode( ChannelHandlerContext ctx, AcquireLocksRequest msg, ByteBuf out ) throws Exception
        {
            GlobalSession.Marshal.INSTANCE.marshal( msg.session, out );

            out.writeInt( msg.mode.ordinal() );
            out.writeBoolean( msg.blocking );
            out.writeInt( msg.typeId );

            out.writeInt( msg.resourceIds.length );
            for ( long resourceId : msg.resourceIds )
            {
                out.writeLong( resourceId );
            }
        }
    }

    public static class Decoder extends ByteToMessageDecoder
    {
        @Override
        public void decode( ChannelHandlerContext ctx, ByteBuf in, List<Object> out ) throws Exception
        {
            GlobalSession session = GlobalSession.Marshal.INSTANCE.unmarshal( in );

            LockMode mode = LockMode.values()[in.readInt()];
            boolean blocking = in.readBoolean();
            int typeId = in.readInt();

            int resourceIdLength = in.readInt();
            long[] resourceIds = new long[resourceIdLength];
            for ( int i = 0; i < resourceIdLength; i++ )
            {
                resourceIds[i] = in.readLong();
            }

            out.add( new AcquireLocksRequest( session, mode, blocking, typeId, resourceIds ) );
        }
    }
}
