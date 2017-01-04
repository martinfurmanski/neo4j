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
package org.neo4j.causalclustering.core.state.machines.locks.client;

import java.util.Optional;
import java.util.concurrent.TimeoutException;
import java.util.function.BiFunction;

import org.neo4j.causalclustering.catchup.CoreCommunicationException;
import org.neo4j.causalclustering.core.replication.Replicator;
import org.neo4j.causalclustering.core.replication.session.LocalSessionPool;
import org.neo4j.causalclustering.core.replication.session.OperationContext;
import org.neo4j.causalclustering.core.state.machines.locks.ContextManager;
import org.neo4j.causalclustering.core.state.machines.locks.LockException;
import org.neo4j.causalclustering.core.state.machines.locks.LockMode;
import org.neo4j.causalclustering.core.state.machines.locks.LockingContext;
import org.neo4j.causalclustering.core.state.machines.locks.SessionAcquisitionException;
import org.neo4j.causalclustering.core.state.machines.locks.net.LockResponse;
import org.neo4j.function.ThrowingAction;
import org.neo4j.function.ThrowingSupplier;
import org.neo4j.kernel.DeadlockDetectedException;
import org.neo4j.kernel.api.exceptions.Status.HasStatus;
import org.neo4j.kernel.impl.locking.Locks;
import org.neo4j.storageengine.api.lock.ResourceType;

import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static org.neo4j.causalclustering.core.state.machines.locks.LockMode.EXCLUSIVE;
import static org.neo4j.causalclustering.core.state.machines.locks.LockMode.SHARED;
import static org.neo4j.causalclustering.core.state.machines.locks.net.LockResponse.Status.SUCCESS;
import static org.neo4j.causalclustering.core.state.machines.locks.token.LockToken.INVALID_LOCK_TOKEN_ID;

/**
 * A core lock client in the core locking system.
 */
public class CoreLockClient implements Locks.Client
{
    private static final long DEFAULT_REPLICATION_TIMEOUT_MS = 60_000;

    private final ContextManager contextManager;
    private final LocalSessionPool sessionPool;
    private final Locks.Client localClient;
    private final Replicator replicator;

    private final LocksCache cache = new LocksCache();

    private LockingContext context;
    private LockSession session;

    public CoreLockClient( ContextManager contextManager, LocalSessionPool sessionPool, Locks.Client localClient, Replicator replicator )
    {
        this.contextManager = contextManager;
        this.sessionPool = sessionPool;
        this.localClient = localClient;
        this.replicator = replicator;
    }

    private void assertValidContext() throws SessionAcquisitionException,
            SessionExpiredException, CoreCommunicationException
    {
        if ( context == null )
        {
            context = contextManager.getContext();
        }
        else if ( !contextManager.validate( context ) )
        {
            throw new SessionExpiredException( "Global context no longer valid" );
        }

        if ( session == null )
        {
            session = acquireSession();
        }
        session.assertValid();
    }

    private LockSession acquireSession() throws SessionAcquisitionException, CoreCommunicationException
    {
        OperationContext operationContext = sessionPool.acquireSession();

        if ( !context.isLocal() )
        {
            LockResponse response = context.rpc().registerSession( operationContext.globalSession() );

            if ( response.status() != SUCCESS )
            {
                throw new SessionAcquisitionException( response.status().toString() );
            }
        }

        return new LockSession( operationContext );
    }

    @Override
    public void acquireShared( ResourceType type, long... ids )
    {
        convertToRuntimeException( this::assertValidContext );

        if ( context.isLocal() )
        {
            localClient.acquireShared( type, ids );
        }
        else
        {
            convertToRuntimeException( () -> acquireBlocking( SHARED, type, ids ) );
        }
    }

    @Override
    public void acquireExclusive( ResourceType type, long... ids )
    {
        convertToRuntimeException( this::assertValidContext );

        if ( context.isLocal() )
        {
            localClient.acquireExclusive( type, ids );
        }
        else
        {
            convertToRuntimeException( () -> acquireBlocking( EXCLUSIVE, type, ids ) );
        }
    }

    @Override
    public boolean trySharedLock( ResourceType type, long id )
    {
        convertToRuntimeException( this::assertValidContext );

        if ( context.isLocal() )
        {
            return localClient.trySharedLock( type, id );
        }
        else
        {
            return convertToRuntimeException( () -> acquireNonBlocking( SHARED, type, id ) );
        }
    }

    @Override
    public boolean tryExclusiveLock( ResourceType type, long id )
    {
        convertToRuntimeException( this::assertValidContext );

        if ( context.isLocal() )
        {
            return localClient.tryExclusiveLock( type, id );
        }
        else
        {
            return convertToRuntimeException( () -> acquireNonBlocking( EXCLUSIVE, type, id ) );
        }
    }

    private void acquireBlocking( LockMode mode, ResourceType type, long... ids )
            throws SessionAcquisitionException, SessionExpiredException,
            LockDataReplicationTimeoutException, CoreCommunicationException
    {
        LockResponse.Status status = acquireLocks( mode, true, type, ids );

        switch ( status )
        {
        case SUCCESS:
            return;
        case E_DEADLOCK:
            throw new DeadlockDetectedException( "Remote deadlock detected." );
        case E_SESSION_INVALID:
            session.expire( "Session has remotely expired." );
            break; // will never be reached
        default:
            throw new IllegalStateException( "Unexpected status code: " + status );
        }
    }

    private boolean acquireNonBlocking( LockMode mode, ResourceType type, long id )
            throws SessionAcquisitionException, SessionExpiredException,
            LockDataReplicationTimeoutException, CoreCommunicationException
    {
        LockResponse.Status status = acquireLocks( mode, false, type, id );

        switch ( status )
        {
        case SUCCESS:
            return true;
        case E_WOULD_BLOCK:
            return false;
        case E_SESSION_INVALID:
            session.expire( "Session has remotely expired." );
            return false; // will never be reached
        default:
            throw new IllegalStateException( "Unexpected status code: " + status );
        }
    }

    private BiFunction<ResourceType,Long,Boolean> tryAcquireLocally( LockMode mode )
    {
        switch ( mode )
        {
        case SHARED:
            return localClient::trySharedLock;
        case EXCLUSIVE:
            return localClient::tryExclusiveLock;
        default:
            throw new IllegalStateException();
        }
    }

    private LockResponse.Status acquireLocks( LockMode mode, boolean blocking, ResourceType type, long... ids )
            throws LockDataReplicationTimeoutException, CoreCommunicationException
    {
        long[] newIds = cache.filter( mode, type, ids );
        LockResponse result = context.rpc().acquireLocks( session.operationContext().globalSession(), mode, blocking, type.typeId(), newIds );

        if ( result.status() != SUCCESS )
        {
            return result.status();
        }

        for ( long id : newIds )
        {
            /* We expect to be able to take the local lock immediately after we
               have the remote lock, hence we use the try-variant. */
            if ( tryAcquireLocally( mode ).apply( type, id ) ) // TODO: Should we acquire locally? Only for schema/entry?
            {
                cache.register( mode, type, id );
            }
            else
            {
                throw new DeadlockDetectedException( "Lock was already locally acquired. This should be transient." );
            }
        }

        if ( result.dataVersion() >= 0 )
        {
            try
            {
                replicator.await( result.dataVersion(), DEFAULT_REPLICATION_TIMEOUT_MS, MILLISECONDS );
            }
            catch ( TimeoutException e )
            {
                throw new LockDataReplicationTimeoutException( e,
                        "Timed out waiting for data to replicate up-to-date with acquired lock." );
            }
        }

        assert result.status() == SUCCESS;
        return SUCCESS;
    }

    @Override
    public void releaseShared( ResourceType type, long id )
    {
        convertToRuntimeException( this::assertValidContext );

        if ( context.isLocal() )
        {
            localClient.releaseShared( type, id );
        }
        else
        {
            convertToRuntimeException( () -> context.rpc().releaseLock( session.operationContext().globalSession(), SHARED, type.typeId(), id ) );
        }
    }

    @Override
    public void releaseExclusive( ResourceType type, long id )
    {
        convertToRuntimeException( this::assertValidContext );

        if ( context.isLocal() )
        {
            localClient.releaseExclusive( type, id );
        }
        else
        {
            convertToRuntimeException( () -> context.rpc().releaseLock( session.operationContext().globalSession(), EXCLUSIVE, type.typeId(), id ) );
        }
    }

    @Override
    public void stop()
    {
        localClient.stop();
        if ( context != null )
        {
            convertToRuntimeException( () -> context.rpc().stopSession( session.operationContext().globalSession() ) );
        }
    }

    @Override
    public void close()
    {
        localClient.close();
        if ( context != null && !context.isLocal() )
        {
            // TODO: End sessions are not possible. They will be remotely ended when the transaction has applied.
            // TODO: But do abort where appropriate (is that stop?).
            convertToRuntimeException( () -> context.rpc().endSession( session.operationContext().globalSession() ) );
        }
    }

    @Override
    public int getLockSessionId()
    {
        return (context != null) ? context.token().id() : INVALID_LOCK_TOKEN_ID;
    }

    /**
     * The exception hierarchy of the system. We throw a general unchecked LockException which
     * should wrap other specialized internal exceptions which implement Status.HasStatus so that
     * connector layers can use the error codes. This keeps the possibility of declaring, catching
     * and handling exceptions cleanly in this module while fulfilling the system level expectation
     * of having a status code in an unchecked exception.
     */
    private static <E extends Exception> void convertToRuntimeException( ThrowingAction<E> action )
    {
        try
        {
            action.apply();
        }
        catch ( Exception e )
        {
            assertCheckedAreNeoExceptions( e );
            throw new LockException( e );
        }
    }

    private static <T, E extends Exception> T convertToRuntimeException( ThrowingSupplier<T,E> supplier )
    {
        try
        {
            return supplier.get();
        }
        catch ( Exception e )
        {
            assertCheckedAreNeoExceptions( e );
            throw new LockException( e );
        }
    }

    /**
     * This is the guarantee we provide. All unchecked exceptions thrown will have a status code.
     *
     * @param e The exception to assert on.
     */
    private static void assertCheckedAreNeoExceptions( Exception e )
    {
        if ( !(e instanceof RuntimeException) )
        {
            assert e instanceof HasStatus;
        }
    }

    public Optional<OperationContext> sessionContext()
    {
        return session == null ? Optional.empty() : Optional.of( session.operationContext() );
    }
}
