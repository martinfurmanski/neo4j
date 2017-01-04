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

import java.time.Clock;

import org.neo4j.causalclustering.catchup.CoreServerProtocol;
import org.neo4j.causalclustering.catchup.ResponseMessageType;
import org.neo4j.causalclustering.core.replication.session.GlobalSession;
import org.neo4j.causalclustering.core.state.machines.locks.LockMode;
import org.neo4j.causalclustering.core.state.machines.locks.net.AcquireLocksRequest;
import org.neo4j.causalclustering.core.state.machines.locks.net.EndSessionRequest;
import org.neo4j.causalclustering.core.state.machines.locks.net.LockResponse;
import org.neo4j.causalclustering.core.state.machines.locks.net.RegisterLockSessionRequest;
import org.neo4j.causalclustering.core.state.machines.locks.net.ReleaseLockRequest;
import org.neo4j.causalclustering.core.state.machines.locks.net.StopSessionRequest;
import org.neo4j.kernel.impl.locking.Locks;
import org.neo4j.kernel.impl.locking.ResourceTypes;
import org.neo4j.kernel.impl.util.collection.ConcurrentAccessException;
import org.neo4j.kernel.impl.util.collection.NoSuchEntryException;
import org.neo4j.kernel.impl.util.collection.TimedRepository;
import org.neo4j.storageengine.api.lock.AcquireLockTimeoutException;

import static org.neo4j.causalclustering.catchup.CoreServerProtocol.State.MESSAGE_TYPE;
import static org.neo4j.causalclustering.core.state.machines.locks.net.LockResponse.Status.E_OPERATION_UNKNOWN;
import static org.neo4j.causalclustering.core.state.machines.locks.net.LockResponse.Status.E_SESSION_INVALID;
import static org.neo4j.causalclustering.core.state.machines.locks.net.LockResponse.Status.E_SESSION_OVERLAP;
import static org.neo4j.causalclustering.core.state.machines.locks.net.LockResponse.Status.E_TIMEOUT;
import static org.neo4j.causalclustering.core.state.machines.locks.net.LockResponse.Status.E_WOULD_BLOCK;
import static org.neo4j.causalclustering.core.state.machines.locks.net.LockResponse.Status.SUCCESS;

// TODO: Return data version
// TODO: Handle session ending/stopping
public class LockServer implements LockRequestHandler
{
    private static final int TIMEOUT = 5000;
    private final TimedRepository<GlobalSession,Proxy> repository = new TimedRepository<>( this::create, this::reap, TIMEOUT, Clock.systemUTC() );

    private final Locks localLocks;

    public LockServer( Locks localLocks )
    {
        this.localLocks = localLocks;
    }

    private Proxy create()
    {
        return new Proxy( localLocks.newClient() );
    }

    private void reap( Proxy proxy )
    {
        proxy.client().close();
    }

    private Operations getLockOps( Locks.Client client, LockMode mode )
    {
        switch ( mode )
        {
        case SHARED:
            return new SharedMode( client );
        case EXCLUSIVE:
            return new ExclusiveMode( client );
        default:
            return null;
        }
    }

    @Override
    public void handle( ChannelHandlerContext ctx, CoreServerProtocol protocol, RegisterLockSessionRequest msg )
    {
        try
        {
            repository.begin( msg.session() );
            sendResponse( ctx, protocol, SUCCESS );
        }
        catch ( ConcurrentAccessException e )
        {
            sendResponse( ctx, protocol, E_SESSION_OVERLAP );
        }
    }

    @Override
    public void handle( ChannelHandlerContext ctx, CoreServerProtocol protocol, AcquireLocksRequest msg )
    {
        Proxy proxy;
        try
        {
            proxy = repository.acquire( msg.session() );
        }
        catch ( NoSuchEntryException e )
        {
            sendResponse( ctx, protocol, E_SESSION_INVALID );
            return;
        }
        catch ( ConcurrentAccessException e )
        {
            sendResponse( ctx, protocol, E_SESSION_OVERLAP );
            return;
        }

        LockResponse.Status status = acquireLocks( proxy.client(), msg.mode(), msg.blocking(), msg.typeId(), msg.resourceIds() );
        sendResponse( ctx, protocol, status );
    }

    private LockResponse.Status acquireLocks( Locks.Client client, LockMode mode, boolean blocking, int typeId, long[] ids )
    {
        ResourceTypes type = ResourceTypes.values()[typeId];

        Operations lockOps = getLockOps( client, mode );

        if ( lockOps == null )
        {
            return E_OPERATION_UNKNOWN;
        }

        return blocking ? blocking( lockOps, type, ids ) : nonBlocking( lockOps, ids, type );
    }

    private LockResponse.Status blocking( Operations ops, ResourceTypes type, long[] resourceIds )
    {
        try
        {
            ops.acquire( type, resourceIds );
        }
        catch ( AcquireLockTimeoutException ignored )
        {
            return E_TIMEOUT;
        }
        return SUCCESS;
    }

    private LockResponse.Status nonBlocking( Operations ops, long[] resourceIds, ResourceTypes type )
    {
        for ( long lastNotAcquired : resourceIds )
        {
            if ( !ops.tryAcquire( type, lastNotAcquired ) )
            {
                releaseUpTo( ops, resourceIds, type, lastNotAcquired );
                return E_WOULD_BLOCK;
            }
        }
        return SUCCESS;
    }

    private void releaseUpTo( Operations ops, long[] resourceIds, ResourceTypes type, long lastAcquired )
    {
        for ( long releaseId : resourceIds )
        {
            if ( releaseId == lastAcquired )
            {
                break;
            }
            ops.release( type, releaseId );
        }
    }

    @Override
    public void handle( ChannelHandlerContext ctx, CoreServerProtocol protocol, ReleaseLockRequest msg )
    {
        Proxy proxy;
        try
        {
            proxy = repository.acquire( msg.session() );
        }
        catch ( NoSuchEntryException e )
        {
            sendResponse( ctx, protocol, E_SESSION_INVALID );
            return;
        }
        catch ( ConcurrentAccessException e )
        {
            sendResponse( ctx, protocol, E_SESSION_OVERLAP );
            return;
        }

        Operations lockOps = getLockOps( proxy.client(), msg.mode() );

        if ( lockOps == null )
        {
            sendResponse( ctx, protocol, E_OPERATION_UNKNOWN );
            return;
        }

        ResourceTypes type = ResourceTypes.values()[msg.typeId()];

        lockOps.release( type, msg.resourceId() );
        sendResponse( ctx, protocol, SUCCESS );
    }

    @Override
    public void handle( ChannelHandlerContext ctx, CoreServerProtocol protocol, StopSessionRequest msg )
    {
        sendResponse( ctx, protocol, SUCCESS ); // TODO
    }

    @Override
    public void handle( ChannelHandlerContext ctx, CoreServerProtocol protocol, EndSessionRequest msg )
    {
        sendResponse( ctx, protocol, SUCCESS ); // TODO
    }

    private void sendResponse( ChannelHandlerContext ctx, CoreServerProtocol protocol, LockResponse.Status status )
    {
        ctx.writeAndFlush( ResponseMessageType.LOCK_RESPONSE );
        ctx.writeAndFlush( new LockResponse( status ) );
        protocol.expect( MESSAGE_TYPE );
    }
}
