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

public class LockResponse
{
    private static final long NO_VERSION = -1;

    public enum Status
    {
        SUCCESS,
        E_WOULD_BLOCK,
        E_TIMEOUT,
        E_DEADLOCK,
        E_SESSION_INVALID,
        E_SESSION_OVERLAP,
        E_OPERATION_UNKNOWN
    }

    private final Status status;
    private final long dataVersion;

    public LockResponse( Status status )
    {
        this( status, NO_VERSION );
    }

    public LockResponse( Status status, long dataVersion )
    {
        assert dataVersion >= NO_VERSION;
        this.status = status;
        this.dataVersion = dataVersion;
    }

    /**
     * The version of the data which must be minimally
     * applied to make the data up-to-date for the taken lock.
     *
     * -1 is used to signify no version
     *
     * (the version corresponds to the consensus log index)
     *
     * @return The data version.
     */
    public long dataVersion()
    {
        return dataVersion;
    }

    public Status status()
    {
        return status;
    }

    public static class Encoder extends MessageToByteEncoder<LockResponse>
    {
        @Override
        protected void encode( ChannelHandlerContext ctx, LockResponse msg, ByteBuf out ) throws Exception
        {
            out.writeInt( msg.status.ordinal() );
            out.writeLong( msg.dataVersion );
        }
    }

    public static class Decoder extends ByteToMessageDecoder
    {
        @Override
        protected void decode( ChannelHandlerContext ctx, ByteBuf in, List<Object> out ) throws Exception
        {
            Status status = Status.values()[in.readInt()];
            long dataVersion = in.readLong();
            out.add( new LockResponse( status, dataVersion ) );
        }
    }
}
