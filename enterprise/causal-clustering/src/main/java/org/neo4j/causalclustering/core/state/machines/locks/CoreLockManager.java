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

import org.neo4j.causalclustering.catchup.CoreClient;
import org.neo4j.causalclustering.core.consensus.LeaderLocator;
import org.neo4j.causalclustering.core.replication.Replicator;
import org.neo4j.causalclustering.core.replication.session.LocalSessionPool;
import org.neo4j.causalclustering.core.state.machines.locks.client.CoreLockClient;
import org.neo4j.causalclustering.core.state.machines.locks.token.ReplicatedLockTokenStateMachine;
import org.neo4j.causalclustering.identity.MemberId;
import org.neo4j.kernel.impl.locking.Locks;

/**
 *
 *        Höðr the blind god Roams about the halls,
 *        For that god he never knows When the darkness falls.
 *        He feeleth on every side, For he cannot see;
 *        Höðr the blind god, A poor god he.
 *
 *
 * A lock manager for the core members allowing locks both for leaders
 * as well as followers.
 *
 * The leader takes on the role of being the central coordination point
 * for all lock requests. This role moves together with a leader switch
 * but not necessarily immediately, but rather upon the first lock
 * request.
 *
 * The locking system implements global one-copy semantics.
 */
public class CoreLockManager implements Locks
{
    private final Locks localLocks;
    private final Replicator replicator;
    private final ContextManager contextManager;
    private LocalSessionPool sessionPool;

    public CoreLockManager( MemberId myIdentity, LocalSessionPool sessionPool, Replicator replicator, LeaderLocator leaderLocator, Locks localLocks, ReplicatedLockTokenStateMachine lockTokenStateMachine, CoreClient coreClient )
    {
        this.sessionPool = sessionPool;
        this.localLocks = localLocks;
        this.replicator = replicator;
        this.contextManager = new ContextManager( myIdentity, leaderLocator, lockTokenStateMachine, coreClient, replicator );
    }

    @Override
    public Client newClient()
    {
        return new CoreLockClient( contextManager, sessionPool, localLocks.newClient(), replicator );
    }

    @Override
    public void accept( Visitor visitor )
    {
        localLocks.accept( visitor );
    }

    @Override
    public void close()
    {
        localLocks.close();
    }
}
