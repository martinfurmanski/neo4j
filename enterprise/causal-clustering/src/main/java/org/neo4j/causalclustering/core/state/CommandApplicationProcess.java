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
package org.neo4j.causalclustering.core.state;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.function.Supplier;

import org.neo4j.causalclustering.SessionTracker;
import org.neo4j.causalclustering.core.consensus.RaftMachine;
import org.neo4j.causalclustering.core.consensus.log.RaftLog;
import org.neo4j.causalclustering.core.consensus.log.RaftLogEntry;
import org.neo4j.causalclustering.core.consensus.log.monitoring.RaftLogCommitIndexMonitor;
import org.neo4j.causalclustering.core.consensus.log.segmented.InFlightMap;
import org.neo4j.causalclustering.core.replication.DistributedOperation;
import org.neo4j.causalclustering.core.replication.ProgressTracker;
import org.neo4j.causalclustering.core.state.machines.CoreStateMachines;
import org.neo4j.causalclustering.core.state.machines.tx.CoreReplicatedContent;
import org.neo4j.causalclustering.core.state.snapshot.CoreSnapshot;
import org.neo4j.causalclustering.core.state.snapshot.CoreStateType;
import org.neo4j.causalclustering.core.state.storage.StateStorage;
import org.neo4j.causalclustering.helper.StatUtil;
import org.neo4j.kernel.internal.DatabaseHealth;
import org.neo4j.kernel.lifecycle.LifecycleAdapter;
import org.neo4j.kernel.monitoring.Monitors;
import org.neo4j.logging.Log;
import org.neo4j.logging.LogProvider;

import static java.lang.Math.max;
import static java.lang.String.format;

public class CommandApplicationProcess extends LifecycleAdapter
{
    private static final long NOTHING = -1;
    private final RaftLog raftLog;
    private final StateStorage<Long> lastFlushedStorage;
    private final int flushEvery;
    private final ProgressTracker progressTracker;
    private final SessionTracker sessionTracker;
    private final Supplier<DatabaseHealth> dbHealth;
    private final InFlightMap<RaftLogEntry> inFlightMap;
    private final Log log;
    private final ControlledTask applyTask;
    private final RaftLogCommitIndexMonitor commitIndexMonitor;
    private final OperationBatcher batcher;
    private StatUtil.StatContext batchStat;

    private CoreStateMachines coreStateMachines;

    private long lastApplied = NOTHING;
    private MonotonicSequenceWaiter applyIndexWaiter = new MonotonicSequenceWaiter( NOTHING );
    private long lastFlushed = NOTHING;
    private volatile boolean abortFlag;

    public CommandApplicationProcess(
            CoreStateMachines coreStateMachines,
            RaftLog raftLog,
            int maxBatchSize,
            int flushEvery,
            Supplier<DatabaseHealth> dbHealth,
            LogProvider logProvider,
            ProgressTracker progressTracker,
            StateStorage<Long> lastFlushedStorage,
            SessionTracker sessionTracker,
            InFlightMap<RaftLogEntry> inFlightMap,
            Monitors monitors )
    {
        this.coreStateMachines = coreStateMachines;
        this.raftLog = raftLog;
        this.lastFlushedStorage = lastFlushedStorage;
        this.flushEvery = flushEvery;
        this.progressTracker = progressTracker;
        this.sessionTracker = sessionTracker;
        this.log = logProvider.getLog( getClass() );
        this.dbHealth = dbHealth;
        this.inFlightMap = inFlightMap;
        this.commitIndexMonitor = monitors.newMonitor( RaftLogCommitIndexMonitor.class, getClass() );
        this.batcher = new OperationBatcher( maxBatchSize );
        this.batchStat = StatUtil.create( "BatchSize", log, 4096, true );
        this.applyTask = new ControlledTask( this::applyTask, "command-applier" );
    }

    synchronized void notifyCommitted( long commitIndex )
    {
        applyIndexWaiter.set( commitIndex );
        commitIndexMonitor.commitIndex( commitIndex );
    }

    private void applyTask()
    {
        long lastIndex = NOTHING;

        try
        {
            lastIndex = applyIndexWaiter.awaitGreaterThan( lastApplied );
            applyUpTo( lastIndex );
        }
        catch ( Throwable e )
        {
            log.error( "Failed to apply up to index " + lastIndex, e );
            dbHealth.get().panic( e );
        }
    }

    private void applyUpTo( long lastIndex ) throws Exception
    {
        try ( InFlightLogEntryReader logEntrySupplier = new InFlightLogEntryReader( raftLog, inFlightMap, true ) )
        {
            for ( long logIndex = lastApplied + 1; logIndex <= lastIndex && !abortFlag; logIndex++ )
            {
                RaftLogEntry entry = logEntrySupplier.get( logIndex );
                if ( entry == null )
                {
                    throw new IllegalStateException( format( "Committed log %d entry must exist.", logIndex ) );
                }

                if ( entry.content() instanceof DistributedOperation )
                {
                    DistributedOperation distributedOperation = (DistributedOperation) entry.content();
                    progressTracker.trackReplication( distributedOperation );
                    batcher.add( logIndex, distributedOperation );
                }
                else
                {
                    batcher.flush();
                    // since this last entry didn't get in the batcher we need to update the lastApplied:
                    lastApplied = logIndex;
                }
            }
            batcher.flush();
        }
    }

    synchronized long lastApplied()
    {
        return lastApplied;
    }

    private void syncApplierAbortOperations()
    {
        abortFlag = true;

        applyIndexWaiter.disable();
        applyTask.pause();
    }

    private void syncApplierNoAbort()
    {
        applyIndexWaiter.disable();
        applyTask.pause();
    }

    public synchronized void sync() throws InterruptedException
    {
        syncApplierAbortOperations();
    }

    private class OperationBatcher
    {
        private List<DistributedOperation> batch;
        private int maxBatchSize;
        private long lastIndex;

        OperationBatcher( int maxBatchSize )
        {
            this.batch = new ArrayList<>( maxBatchSize );
            this.maxBatchSize = maxBatchSize;
        }

        private void add( long index, DistributedOperation operation ) throws Exception
        {
            if ( batch.size() > 0 )
            {
                assert index == (lastIndex + 1);
            }

            batch.add( operation );
            lastIndex = index;

            if ( batch.size() == maxBatchSize )
            {
                flush();
            }
        }

        private void flush() throws Exception
        {
            if ( batch.size() == 0 )
            {
                return;
            }

            batchStat.collect( batch.size() );

            long startIndex = lastIndex - batch.size() + 1;
            long lastHandledIndex = handleOperations( startIndex, batch );
            assert lastHandledIndex == lastIndex;
            lastApplied = lastIndex;

            batch.clear();
            maybeFlush();
        }
    }

    public void prune() throws IOException
    {
        raftLog.prune( lastFlushed );
    }

    private long handleOperations( long commandIndex, List<DistributedOperation> operations )
    {
        try ( CommandDispatcher dispatcher = coreStateMachines.commandDispatcher() )
        {
            for ( DistributedOperation operation : operations )
            {
                if ( !sessionTracker.validateOperation( operation.globalSession(), operation.operationId() ) )
                {
                    sessionTracker.validateOperation( operation.globalSession(), operation.operationId() );
                    commandIndex++;
                    continue;
                }

                CoreReplicatedContent command = (CoreReplicatedContent) operation.content();
                command.dispatch( dispatcher, commandIndex,
                        result -> progressTracker.trackResult( operation, result ) );

                sessionTracker.update( operation.globalSession(), operation.operationId(), commandIndex );
                commandIndex++;
            }
        }
        return commandIndex - 1;
    }

    private void maybeFlush() throws IOException
    {
        if ( (lastApplied - lastFlushed) > flushEvery )
        {
            flush();
        }
    }

    private void flush() throws IOException
    {
        coreStateMachines.flush();
        sessionTracker.flush();
        lastFlushedStorage.persistStoreData( lastApplied );
        lastFlushed = lastApplied;
    }

    @Override
    public synchronized void start() throws IOException, InterruptedException
    {
        // TODO: check None/Partial/Full here, because this is the first level which can
        // TODO: bootstrapping RAFT can also be performed from here.

        if ( lastFlushed == NOTHING )
        {
            lastFlushed = lastFlushedStorage.getInitialState();
        }
        lastApplied = lastFlushed;

        log.info( format( "Restoring last applied index to %d", lastApplied ) );
        sessionTracker.start();

        /* Considering the order in which state is flushed, the state machines will
         * always be furthest ahead and indicate the furthest possible state to
         * which we must replay to reach a consistent state. */
        applyIndexWaiter.set( max( coreStateMachines.getLastAppliedIndex(), sessionTracker.getLastAppliedIndex() ) );
        syncApplierNoAbort();
    }

    @Override
    public synchronized void stop() throws InterruptedException, IOException
    {
        syncApplierAbortOperations();
        flush();
    }

    public synchronized CoreSnapshot snapshot( RaftMachine raft ) throws IOException, InterruptedException
    {
        syncApplierNoAbort();

        long prevIndex = lastApplied;
        long prevTerm = raftLog.readEntryTerm( prevIndex );
        CoreSnapshot coreSnapshot = new CoreSnapshot( prevIndex, prevTerm );

        coreStateMachines.addSnapshots( coreSnapshot );
        coreSnapshot.add( CoreStateType.SESSION_TRACKER, sessionTracker.snapshot() );
        coreSnapshot.add( CoreStateType.RAFT_CORE_STATE, raft.coreState() );

        return coreSnapshot;
    }

    synchronized void installSnapshot( CoreSnapshot coreSnapshot, RaftMachine raft ) throws IOException
    {
        coreStateMachines.installSnapshots( coreSnapshot );
        long snapshotPrevIndex = coreSnapshot.prevIndex();
        try
        {
            raftLog.skip( snapshotPrevIndex, coreSnapshot.prevTerm() );
        }
        catch ( IOException e )
        {
            throw new RuntimeException( e );
        }
        lastApplied = lastFlushed = snapshotPrevIndex;
        log.info( format( "Skipping lastApplied index forward to %d", snapshotPrevIndex ) );

        raft.installCoreState( coreSnapshot.get( CoreStateType.RAFT_CORE_STATE ) );

        sessionTracker.installSnapshot( coreSnapshot.get( CoreStateType.SESSION_TRACKER ) );
        flush();
    }
}
