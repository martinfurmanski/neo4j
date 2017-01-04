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
package org.neo4j.causalclustering.core.state.machines.locks;

import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;

import org.neo4j.causalclustering.catchup.CoreClient;
import org.neo4j.causalclustering.core.consensus.LeaderLocator;
import org.neo4j.causalclustering.core.consensus.NoLeaderFoundException;
import org.neo4j.causalclustering.core.replication.Replicator;
import org.neo4j.causalclustering.core.state.machines.locks.net.RemoteLocksImpl;
import org.neo4j.causalclustering.core.state.machines.locks.token.LockToken;
import org.neo4j.causalclustering.core.state.machines.locks.token.ReplicatedLockToken;
import org.neo4j.causalclustering.core.state.machines.locks.token.ReplicatedLockTokenStateMachine;
import org.neo4j.causalclustering.identity.MemberId;

import static org.neo4j.causalclustering.core.state.machines.locks.token.ReplicatedLockToken.INVALID_REPLICATED_LOCK_TOKEN;

public class ContextManager
{
    private final LeaderLocator leaderLocator;
    private final ReplicatedLockTokenStateMachine lockTokenStateMachine;
    private final CoreClient coreClient;
    private final Replicator replicator;
    private volatile LockingContext currentContext;
    private MemberId myIdentity;

    ContextManager( MemberId myIdentity, LeaderLocator leaderLocator, ReplicatedLockTokenStateMachine lockTokenStateMachine, CoreClient coreClient, Replicator replicator )
    {
        this.myIdentity = myIdentity;
        this.leaderLocator = leaderLocator;
        this.lockTokenStateMachine = lockTokenStateMachine;
        this.coreClient = coreClient;
        this.replicator = replicator;
        this.currentContext = new LockingContext( INVALID_REPLICATED_LOCK_TOKEN, null, false );
    }

    private MemberId getLeader() throws SessionAcquisitionException
    {
        MemberId leader;
        try
        {
            leader = leaderLocator.getLeader();
        }
        catch ( NoLeaderFoundException e )
        {
            throw new SessionAcquisitionException( e );
        }
        return leader;
    }

    public LockingContext getContext() throws SessionAcquisitionException
    {
        MemberId leader = getLeader();

        if ( !leader.equals( currentContext.token().owner() ) )
        {
            // TODO: Rate limit this call?
            refreshContext( leader );
        }

        return currentContext;
    }

    private synchronized void refreshContext( MemberId leader ) throws SessionAcquisitionException
    {
        if ( leader.equals( currentContext.token().owner() ) )
        {
            return;
        }

        LockToken newToken = requestTokenFor( leader );
        RemoteLocksImpl rpc = new RemoteLocksImpl( leader, myIdentity, coreClient );
        boolean isLocal = myIdentity.equals( leader );

        currentContext = new LockingContext( newToken, rpc, isLocal );
    }

    private LockToken requestTokenFor( MemberId leader ) throws SessionAcquisitionException
    {
        ReplicatedLockToken lockTokenRequest = new ReplicatedLockToken( leader,
                LockToken.nextCandidateId( lockTokenStateMachine.currentToken().id() ) );

        Future<Object> futureResult;
        try
        {
            // TODO: Think about a timeout
            futureResult = replicator.replicate( lockTokenRequest, true );
        }
        catch ( InterruptedException e )
        {
            Thread.currentThread().interrupt();
            throw new SessionAcquisitionException( e );
        }

        try
        {
            boolean success = (boolean) futureResult.get();
            if ( success )
            {
                return lockTokenRequest;
            }
            else
            {
                throw new SessionAcquisitionException( "Failed to acquire lock token. Was taken by another candidate." );
            }
        }
        catch ( ExecutionException e )
        {
            throw new SessionAcquisitionException( e );
        }
        catch ( InterruptedException e )
        {
            Thread.currentThread().interrupt();
            throw new SessionAcquisitionException( e );
        }
    }

    public boolean validate( LockingContext context )
    {
        return lockTokenStateMachine.currentToken().equals( context.token() );
    }
}
