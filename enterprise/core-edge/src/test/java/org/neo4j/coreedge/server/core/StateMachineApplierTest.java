/*
 * Copyright (c) 2002-2016 "Neo Technology,"
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
package org.neo4j.coreedge.server.core;

import java.util.function.Supplier;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.mockito.internal.verification.VerificationModeFactory;
import org.mockito.verification.Timeout;
import org.mockito.verification.VerificationWithTimeout;

import org.neo4j.coreedge.raft.log.InMemoryRaftLog;
import org.neo4j.coreedge.raft.log.RaftLogEntry;
import org.neo4j.coreedge.raft.state.InMemoryStateStorage;
import org.neo4j.coreedge.raft.state.LastAppliedState;
import org.neo4j.coreedge.raft.state.StateMachine;
import org.neo4j.coreedge.raft.state.StateMachineApplier;
import org.neo4j.kernel.internal.DatabaseHealth;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.timeout;
import static org.mockito.Mockito.verify;

import static org.neo4j.coreedge.raft.ReplicatedInteger.valueOf;
import static org.neo4j.logging.NullLogProvider.getInstance;

public class StateMachineApplierTest
{
    private InMemoryRaftLog raftLog = new InMemoryRaftLog();
    private StateMachineApplier applier;
    private StateMachine stateMachine;
    private InMemoryStateStorage<LastAppliedState> lastApplied;

    @Test
    public void shouldApplyCommittedCommands() throws Exception
    {
        // given
        raftLog.append( new RaftLogEntry( 0, valueOf( 0 ) ) );
        raftLog.commit( 0 );
        applier.start();

        // when
        applier.notifyCommitted();

        // then
        verify( stateMachine, soon() ).applyCommand( valueOf( 0 ), 0 );
    }

    @Test
    public void shouldNotApplyAnythingIfNothingIsCommitted() throws Exception
    {
        // given
        applier.start();

        // when
        applier.notifyCommitted();

        // then
        verify( stateMachine, never() ).applyCommand( valueOf( 0 ), 0 );
    }

    @Test
    public void startShouldApplyCommittedButUnAppliedCommands() throws Exception
    {
        // given
        raftLog.append( new RaftLogEntry( 0, valueOf( 0 ) ) );
        raftLog.append( new RaftLogEntry( 0, valueOf( 1 ) ) );
        raftLog.append( new RaftLogEntry( 0, valueOf( 2 ) ) );
        raftLog.append( new RaftLogEntry( 0, valueOf( 3 ) ) );
        raftLog.commit( 3 );

        lastApplied.persistStoreData( new LastAppliedState( 1 ) );

        // when
        applier.start();

        // then
        verify( stateMachine ).applyCommand( valueOf( 2 ), 2 );
        verify( stateMachine ).applyCommand( valueOf( 3 ), 3 );
    }

    private static VerificationWithTimeout soon()
    {
        return timeout( 10 );
    }

    private static Timeout never()
    {
        return new Timeout( 10, VerificationModeFactory.noMoreInteractions() );
    }

    @Before
    public void setup() throws Exception
    {
        stateMachine = mock( StateMachine.class );
        lastApplied = new InMemoryStateStorage<>( new LastAppliedState( -1 ) );
        applier = new StateMachineApplier( stateMachine, raftLog, lastApplied, 1, getInstance(), health() );
    }

    @After
    public void tearDown() throws Exception
    {
        applier.stop();
    }

    @SuppressWarnings("unchecked")
    private Supplier<DatabaseHealth> health()
    {
        return mock( Supplier.class );
    }
}
