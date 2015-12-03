/*
 * Copyright (c) 2002-2015 "Neo Technology,"
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
package org.neo4j.coreedge.raft.replication.tx;

import java.io.IOException;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Future;

import org.neo4j.concurrent.CompletableFuture;
import org.neo4j.coreedge.raft.replication.ReplicatedContent;
import org.neo4j.coreedge.raft.replication.Replicator;
import org.neo4j.coreedge.raft.replication.session.GlobalSession;
import org.neo4j.coreedge.raft.replication.session.GlobalSessionTracker;
import org.neo4j.coreedge.raft.replication.session.LocalOperationId;
import org.neo4j.graphdb.TransientFailureException;
import org.neo4j.kernel.NeoStoreDataSource;
import org.neo4j.kernel.api.exceptions.TransactionFailureException;
import org.neo4j.kernel.impl.api.TransactionApplicationMode;
import org.neo4j.kernel.impl.api.TransactionCommitProcess;
import org.neo4j.kernel.impl.api.TransactionToApply;
import org.neo4j.kernel.impl.transaction.CommittedTransactionRepresentation;
import org.neo4j.kernel.impl.transaction.TransactionRepresentation;
import org.neo4j.kernel.impl.transaction.log.IOCursor;
import org.neo4j.kernel.impl.transaction.log.LogicalTransactionStore;
import org.neo4j.kernel.impl.transaction.tracing.CommitEvent;
import org.neo4j.kernel.impl.util.Dependencies;

public class ReplicatedTransactionStateMachine implements Replicator.ReplicatedContentListener
{
    private final GlobalSessionTracker sessionTracker;
    private final GlobalSession myGlobalSession;
    private final Dependencies dependencies;
    private final TransactionCommitProcess commitProcess;
    private final Map<LocalOperationId, FutureTxId> outstanding = new ConcurrentHashMap<>();
    private long lastIndexCommitted = -1;

    private boolean recoveryMode;

    public ReplicatedTransactionStateMachine( TransactionCommitProcess commitProcess,
            GlobalSession myGlobalSession, Dependencies dependencies )
    {
        this.commitProcess = commitProcess;
        this.myGlobalSession = myGlobalSession;
        this.dependencies = dependencies;
        this.sessionTracker = new GlobalSessionTracker();
    }

    public Future<Long> getFutureTxId( LocalOperationId localOperationId )
    {
        final FutureTxId future = new FutureTxId( localOperationId );
        outstanding.put( localOperationId, future );
        return future;
    }

    @Override
    public synchronized void onReplicated( ReplicatedContent content, long index )
    {
        if ( content instanceof ReplicatedTransaction )
        {
            handleTransaction( (ReplicatedTransaction) content, index );
        }
    }

    private void handleTransaction( ReplicatedTransaction replicatedTransaction, long index )
    {
        try
        {
            if ( sessionTracker.validateAndTrackOperation( replicatedTransaction.globalSession(),
                    replicatedTransaction.localOperationId() ) )
            {
                if ( recoveryMode )
                {
                    recoveryVersion( replicatedTransaction, index );
                }
                else
                {
                    normalVersion( replicatedTransaction, index );
                }
            }
        }
        catch ( TransactionFailureException | IOException e )
        {
            throw new IllegalStateException( "Failed to locally commit a transaction that has already been " +
                    "committed to the RAFT log. This server cannot process later transactions and needs to be " +
                    "restarted once the underlying cause has been addressed.", e );
        }
    }

    private void recoveryVersion( ReplicatedTransaction replicatedTransaction, long index ) throws IOException, TransactionFailureException
    {
        if ( index > lastIndexCommitted )
        {
            byte[] extraHeader = longToBytes( index );
            TransactionRepresentation tx = ReplicatedTransactionFactory.extractTransactionRepresentation(
                    replicatedTransaction, extraHeader );

            try
            {
                commitProcess.commit( new TransactionToApply( tx ), CommitEvent.NULL, TransactionApplicationMode.EXTERNAL );
            }
            catch ( TransientFailureException e )
            {
                // TODO: Consider logging?
            }
        }
    }

    private void normalVersion( ReplicatedTransaction replicatedTransaction, long index ) throws IOException, TransactionFailureException
    {
        byte[] extraHeader = longToBytes( index );
        TransactionRepresentation tx = ReplicatedTransactionFactory.extractTransactionRepresentation(
                replicatedTransaction, extraHeader );

        Optional<CompletableFuture<Long>> future = Optional.empty(); // A missing future means the transaction does not belong to this instance
        if ( replicatedTransaction.globalSession().equals( myGlobalSession ) )
        {
            future = Optional.of( outstanding.remove( replicatedTransaction.localOperationId() ) );
        }

        try
        {
            long txId = commitProcess.commit( new TransactionToApply( tx ), CommitEvent.NULL, TransactionApplicationMode.EXTERNAL );
            future.ifPresent( txFuture -> txFuture.complete( txId ) );
        }
        catch ( TransientFailureException e )
        {
            future.ifPresent( txFuture -> txFuture.completeExceptionally( e ) );
        }
    }

    public void setRecoveryMode( long lastIndexCommitted )
    {
        this.lastIndexCommitted = lastIndexCommitted;
        recoveryMode = true;
    }

    public void unsetRecoveryMode()
    {
        recoveryMode = false;
    }

    private class FutureTxId extends CompletableFuture<Long>
    {
        private final LocalOperationId localOperationId;

        FutureTxId( LocalOperationId localOperationId )
        {
            this.localOperationId = localOperationId;
        }

        @Override
        public boolean cancel( boolean mayInterruptIfRunning )
        {
            if ( !super.cancel( mayInterruptIfRunning ) )
            {
                return false;
            }
            outstanding.remove( localOperationId );
            return true;
        }
    }

    public static byte[] longToBytes( long theLong )
    {
        final int bytesPerLong = Long.BYTES / Byte.BYTES;
        byte[] b = new byte[bytesPerLong];
        for ( int i = bytesPerLong - 1; i > 0; i-- )
        {
            b[i] = (byte) theLong;
            theLong >>>= 8;
        }
        b[0] = (byte) theLong;
        return b;
    }

    public static long bytesToLong(byte[] bytes )
    {
        long result = 0;
        for ( int i = 0; i < 8; i++ )
        {
            result <<= 8;
            result ^= bytes[i] & 0xFF;
        }
        return result;
    }
}
