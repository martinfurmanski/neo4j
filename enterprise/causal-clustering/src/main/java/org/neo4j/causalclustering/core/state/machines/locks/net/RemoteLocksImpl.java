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

import java.util.concurrent.CompletableFuture;

import org.neo4j.causalclustering.catchup.CoreClient;
import org.neo4j.causalclustering.catchup.CoreClientResponseAdaptor;
import org.neo4j.causalclustering.catchup.CoreCommunicationException;
import org.neo4j.causalclustering.core.replication.session.GlobalSession;
import org.neo4j.causalclustering.core.state.machines.locks.LockMode;
import org.neo4j.causalclustering.identity.MemberId;

public class RemoteLocksImpl implements RemoteLocks
{
    private final MemberId lockManagerId;
    private final MemberId localMemberId;
    private final CoreClient client;

    public RemoteLocksImpl( MemberId lockManagerId, MemberId myIdentity, CoreClient client )
    {
        this.lockManagerId = lockManagerId;
        this.localMemberId = myIdentity;
        this.client = client;
    }

    @Override
    public LockResponse registerSession( GlobalSession session ) throws CoreCommunicationException
    {
        RegisterLockSessionRequest request = new RegisterLockSessionRequest( session );
        return client.makeBlockingRequest( lockManagerId, request, new CoreClientResponseAdaptor<LockResponse>()
        {
            @Override
            public void onLockResponse( CompletableFuture<LockResponse> signal, LockResponse response )
            {
                signal.complete( response );
            }
        } );
    }

    @Override
    public LockResponse acquireLocks( GlobalSession session, LockMode mode, boolean blocking, int typeId, long[] resourceIds ) throws CoreCommunicationException
    {
        AcquireLocksRequest request = new AcquireLocksRequest( session, mode, blocking, typeId, resourceIds );
        return client.makeBlockingRequest( lockManagerId, request, new CoreClientResponseAdaptor<LockResponse>()
        {
            @Override
            public void onLockResponse( CompletableFuture<LockResponse> signal, LockResponse response )
            {
                signal.complete( response );
            }
        } );
    }

    @Override
    public LockResponse releaseLock( GlobalSession session, LockMode mode, int typeId, long id ) throws CoreCommunicationException
    {
        ReleaseLockRequest request = new ReleaseLockRequest( session, mode, typeId, id );
        return client.makeBlockingRequest( lockManagerId, request, new CoreClientResponseAdaptor<LockResponse>()
        {
            @Override
            public void onLockResponse( CompletableFuture<LockResponse> signal, LockResponse response )
            {
                signal.complete( response );
            }
        } );
    }

    @Override
    public LockResponse stopSession( GlobalSession session ) throws CoreCommunicationException
    {
        StopSessionRequest request = new StopSessionRequest( session );
        return client.makeBlockingRequest( lockManagerId, request, new CoreClientResponseAdaptor<LockResponse>()
        {
            @Override
            public void onLockResponse( CompletableFuture<LockResponse> signal, LockResponse response )
            {
                signal.complete( response );
            }
        } );
    }

    @Override
    public LockResponse endSession( GlobalSession session ) throws CoreCommunicationException
    {
        EndSessionRequest request = new EndSessionRequest( session );
        return client.makeBlockingRequest( lockManagerId, request, new CoreClientResponseAdaptor<LockResponse>()
        {
            @Override
            public void onLockResponse( CompletableFuture<LockResponse> signal, LockResponse response )
            {
                signal.complete( response );
            }
        } );
    }

    @Override
    public String toString()
    {
        return "RemoteLocksImpl{" +
               "lockManagerId=" + lockManagerId +
               ", localMemberId=" + localMemberId +
               '}';
    }
}
