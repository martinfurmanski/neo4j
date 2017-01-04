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
package org.neo4j.causalclustering.core.state.machines.locks.server;

import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;
import io.netty.util.ReferenceCountUtil;

import org.neo4j.causalclustering.catchup.CoreServerProtocol;
import org.neo4j.causalclustering.core.state.machines.locks.net.AcquireLocksRequest;
import org.neo4j.causalclustering.core.state.machines.locks.net.EndSessionRequest;
import org.neo4j.causalclustering.core.state.machines.locks.net.RegisterLockSessionRequest;
import org.neo4j.causalclustering.core.state.machines.locks.net.ReleaseLockRequest;
import org.neo4j.causalclustering.core.state.machines.locks.net.StopSessionRequest;

public class LockRequestDispatcher extends ChannelInboundHandlerAdapter
{
    private final CoreServerProtocol protocol;
    private final LockRequestHandler handler;

    public LockRequestDispatcher( CoreServerProtocol protocol, LockRequestHandler handler )
    {
        this.protocol = protocol;
        this.handler = handler;
    }

    @Override
    public void channelRead( ChannelHandlerContext ctx, Object msg ) throws Exception
    {
        boolean release = true;
        try
        {
            if ( !dispatch( ctx, msg ) )
            {
                release = false;
                ctx.fireChannelRead( msg );
            }
        }
        finally
        {
            if ( release )
            {
                ReferenceCountUtil.release( msg );
            }
        }
    }

    private boolean dispatch( ChannelHandlerContext ctx, Object msg )
    {
        if ( msg instanceof AcquireLocksRequest )
        {
            handler.handle( ctx, protocol, (AcquireLocksRequest) msg );
        }
        else if ( msg instanceof ReleaseLockRequest )
        {
            handler.handle( ctx, protocol, (ReleaseLockRequest) msg );
        }
        else if ( msg instanceof RegisterLockSessionRequest )
        {
            handler.handle( ctx, protocol, (RegisterLockSessionRequest) msg );
        }
        else if ( msg instanceof EndSessionRequest )
        {
            handler.handle( ctx, protocol, (EndSessionRequest) msg );
        }
        else if ( msg instanceof StopSessionRequest )
        {
            handler.handle( ctx, protocol, (StopSessionRequest) msg );
        }
        else
        {
            return false;
        }
        return true;
    }
}
