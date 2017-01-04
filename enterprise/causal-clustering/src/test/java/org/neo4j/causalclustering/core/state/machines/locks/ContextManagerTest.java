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

import org.junit.Ignore;
import org.junit.Test;

import java.util.UUID;

import org.neo4j.causalclustering.catchup.CoreClient;
import org.neo4j.causalclustering.core.consensus.LeaderLocator;
import org.neo4j.causalclustering.core.consensus.NoLeaderFoundException;
import org.neo4j.causalclustering.core.replication.DirectReplicator;
import org.neo4j.causalclustering.core.replication.Replicator;
import org.neo4j.causalclustering.core.state.machines.locks.token.ReplicatedLockTokenState;
import org.neo4j.causalclustering.core.state.machines.locks.token.ReplicatedLockTokenStateMachine;
import org.neo4j.causalclustering.core.state.storage.InMemoryStateStorage;
import org.neo4j.causalclustering.core.state.storage.StateStorage;
import org.neo4j.causalclustering.identity.MemberId;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class ContextManagerTest
{
    private LeaderLocator leaderLocator = mock( LeaderLocator.class );
    private StateStorage<ReplicatedLockTokenState> storage = new InMemoryStateStorage<>( new ReplicatedLockTokenState() );
    private ReplicatedLockTokenStateMachine lockTokenStateMachine = new ReplicatedLockTokenStateMachine( storage );
    private Replicator replicator = new DirectReplicator<>( lockTokenStateMachine );
    private final MemberId myIdentity = new MemberId( UUID.randomUUID() );

    private ContextManager contextManager = new ContextManager( myIdentity, leaderLocator,
            lockTokenStateMachine, mock( CoreClient.class ), replicator );

    @Test( expected = SessionAcquisitionException.class )
    public void shouldThrowIfNoLeader() throws Exception
    {
        // given
        when( leaderLocator.getLeader() ).thenThrow( new NoLeaderFoundException() );

        // when
        contextManager.getContext();
    }

    @Test
    public void shouldRequestNewTokenWhenInvalid() throws Exception
    {
        // given
        MemberId leaderA = new MemberId( UUID.randomUUID() );
        MemberId leaderB = new MemberId( UUID.randomUUID() );

        // when: first get
        when( leaderLocator.getLeader() ).thenReturn( leaderA );
        LockingContext contextA = contextManager.getContext();

        // then: token owned by leader
        assertEquals( leaderA, contextA.owner() );
        assertEquals( 0, contextA.token().id() );

        // when: getting again
        contextA = contextManager.getContext();

        // then: still the same
        assertEquals( leaderA, contextA.owner() );
        assertEquals( 0, contextA.token().id() );

        // when: leader change
        when( leaderLocator.getLeader() ).thenReturn( leaderB );
        LockingContext contextB = contextManager.getContext();

        // then: new token
        assertEquals( leaderB, contextB.owner() );
        assertEquals( 1, contextB.token().id() );

        assertFalse( contextManager.validate( contextA ) );
        assertTrue( contextManager.validate( contextB ) );
    }

    @Ignore
    @Test( expected = SessionAcquisitionException.class )
    public void shouldFailAcquisitionWhenAnotherMemberInterferes() throws Exception
    {
        // given

        // when

        // then
    }
}
