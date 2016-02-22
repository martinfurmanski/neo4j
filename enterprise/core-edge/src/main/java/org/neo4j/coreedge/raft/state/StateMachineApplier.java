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
package org.neo4j.coreedge.raft.state;

import java.io.IOException;
import java.util.function.Supplier;

import org.neo4j.coreedge.raft.ConsensusListener;
import org.neo4j.coreedge.raft.log.RaftLogEntry;
import org.neo4j.coreedge.raft.log.RaftStorageException;
import org.neo4j.coreedge.raft.log.ReadableRaftLog;
import org.neo4j.kernel.internal.DatabaseHealth;
import org.neo4j.kernel.lifecycle.LifecycleAdapter;
import org.neo4j.logging.Log;
import org.neo4j.logging.LogProvider;

import static java.lang.System.currentTimeMillis;

public class StateMachineApplier extends LifecycleAdapter implements ConsensusListener
{
    public static final long NOTHING_APPLIED = -1;

    private final StateMachine stateMachine;
    private final ReadableRaftLog raftLog;
    private final StateStorage<LastAppliedState> lastAppliedStorage;
    private final int flushEvery;
    private final Supplier<DatabaseHealth> dbHealth;
    private final Log log;
    private long lastApplied = NOTHING_APPLIED;

    private Thread applierThread;
    private boolean applierShouldBeRunning;

    private long commitIndex = NOTHING_APPLIED;

    public StateMachineApplier( StateMachine stateMachine, ReadableRaftLog raftLog, StateStorage<LastAppliedState> lastAppliedStorage,
            int flushEvery, LogProvider logProvider, Supplier<DatabaseHealth> dbHealth )
    {
        this.stateMachine = stateMachine;
        this.raftLog = raftLog;
        this.lastAppliedStorage = lastAppliedStorage;
        this.flushEvery = flushEvery;
        this.log = logProvider.getLog( getClass() );
        this.dbHealth = dbHealth;

        applierThread = new Thread( this::applyThreadRunner, "state-machine-applier" );
    }

    @Override
    public synchronized void notifyCommitted()
    {
        if( this.commitIndex != raftLog.commitIndex() )
        {
            this.commitIndex = raftLog.commitIndex();
            notifyAll();
        }
    }

    private synchronized long waitForCommit() throws InterruptedException
    {
        while( lastApplied == commitIndex && applierShouldBeRunning )
        {
            wait();
        }
        return commitIndex;
    }

    private void applyThreadRunner()
    {
        while( applierShouldBeRunning )
        {
            try
            {
                long commitIndex = waitForCommit();
                applyUpTo( commitIndex );
            }
            catch ( InterruptedException e )
            {
                log.warn( "Applier unexpectedly interrupted", e );
            }
            catch ( RaftStorageException | IOException e )
            {
                log.error( "Applier had storage exception", e );
                dbHealth.get().panic( e );
                applierShouldBeRunning = false;
            }
        }
    }

    private void applyUpTo( long commitIndex ) throws IOException, RaftStorageException
    {
        while ( lastApplied < commitIndex )
        {
            long indexToApply = lastApplied + 1;

            RaftLogEntry logEntry = raftLog.readLogEntry( indexToApply );
            stateMachine.applyCommand( logEntry.content(), indexToApply );

            lastApplied = indexToApply;

            if ( indexToApply % this.flushEvery == 0 )
            {
                stateMachine.flush();
                lastAppliedStorage.persistStoreData( new LastAppliedState( lastApplied ) );
            }
        }
    }

    @Override
    public synchronized void start() throws IOException, RaftStorageException
    {
        lastApplied = lastAppliedStorage.getInitialState().get();
        log.info( "Replaying commands from index %d to index %d", lastApplied, raftLog.commitIndex() );

        long start = currentTimeMillis();
        applyUpTo( raftLog.commitIndex() );
        log.info( "Replay done, took %d ms", currentTimeMillis() - start );

        applierThread = new Thread( this::applyThreadRunner, "state-machine-applier" );
        applierShouldBeRunning = true;
        applierThread.start();
    }

    private synchronized void stopApplier()
    {
        applierShouldBeRunning = false;
        notifyAll();
    }

    @Override
    public void stop() throws InterruptedException
    {
        stopApplier();
        applierThread.join();
    }
}
