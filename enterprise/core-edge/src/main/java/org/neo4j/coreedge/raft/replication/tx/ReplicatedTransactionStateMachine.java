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
package org.neo4j.coreedge.raft.replication.tx;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.function.Consumer;

import org.neo4j.coreedge.raft.state.Result;
import org.neo4j.coreedge.raft.state.StateMachine;
import org.neo4j.coreedge.server.core.locks.ReplicatedLockTokenStateMachine;
import org.neo4j.kernel.api.exceptions.TransactionFailureException;
import org.neo4j.kernel.impl.api.TransactionCommitProcess;
import org.neo4j.kernel.impl.api.TransactionToApply;
import org.neo4j.kernel.impl.locking.Locks;
import org.neo4j.kernel.impl.transaction.TransactionRepresentation;
import org.neo4j.kernel.impl.transaction.tracing.CommitEvent;
import org.neo4j.logging.Log;
import org.neo4j.logging.LogProvider;
import org.neo4j.storageengine.api.TransactionApplicationMode;

import static java.util.concurrent.TimeUnit.MINUTES;
import static org.neo4j.coreedge.raft.replication.tx.LogIndexTxHeaderEncoding.encodeLogIndexAsTxHeader;
import static org.neo4j.kernel.api.exceptions.Status.Transaction.LockSessionExpired;

public class ReplicatedTransactionStateMachine<MEMBER> implements StateMachine<ReplicatedTransaction>
{
    private final ReplicatedLockTokenStateMachine<MEMBER> lockTokenStateMachine;
    private TransactionCommitProcess commitProcess;
    private final Log log;

    private long lastCommittedIndex = -1;

    public ReplicatedTransactionStateMachine( ReplicatedLockTokenStateMachine<MEMBER> lockStateMachine,
                                              LogProvider logProvider )
    {
        this.lockTokenStateMachine = lockStateMachine;
        this.log = logProvider.getLog( getClass() );

        new Thread( ReplicatedTransactionStateMachine.this::batchCommitProcess ).start();
    }

    public synchronized void installCommitProcess( TransactionCommitProcess commitProcess, long lastCommittedIndex )
    {
        this.commitProcess = commitProcess;
        this.lastCommittedIndex = lastCommittedIndex;
    }

    @Override
    public synchronized void flush() throws IOException
    {
        // implicity flushed
    }

    class TxContext
    {
        private Consumer<Result> consumer;
        private TransactionRepresentation tx;

        TxContext( Consumer<Result> consumer, TransactionRepresentation tx )
        {
            this.consumer = consumer;
            this.tx = tx;
        }
    }

    private BlockingQueue<TxContext> txQ = new ArrayBlockingQueue<>( 1024 );

    private int MAX_BATCH = 64;
    private List<TxContext> contextList = new ArrayList<>( MAX_BATCH );
    private boolean isRunning = true;

    private void batchCommitProcess()
    {
        while ( isRunning )
        {
            TxContext firstContext = null;
            try
            {
                firstContext = txQ.poll( 1, MINUTES );
            }
            catch ( InterruptedException e )
            {
                // ignored
            }

            if ( firstContext != null )
            {
                contextList.add( firstContext );
                txQ.drainTo( contextList, MAX_BATCH - 1 );

                TransactionToApply first = null;
                TransactionToApply last = null;

                for ( TxContext context : contextList )
                {
                    if( first == null )
                    {
                        first = last = new TransactionToApply( context.tx );
                    }
                    else
                    {
                        TransactionToApply next = new TransactionToApply( context.tx );
                        last.next( next );
                        last = next;
                    }
                }

                long txId = -1;
                try
                {
                    txId = commitProcess.commit( first, CommitEvent.NULL, TransactionApplicationMode.EXTERNAL );
                }
                catch ( TransactionFailureException e )
                {
                    e.printStackTrace();
                }

                Result result = Result.of( txId );
                for ( TxContext aTx : contextList )
                {
                    aTx.consumer.accept( result );
                }

                contextList.clear();
            }
        }
    }

    @Override
    public synchronized void applyCommand( ReplicatedTransaction replicatedTx, long commandIndex, Consumer<Result> consumer )
    {
        if ( commandIndex <= lastCommittedIndex )
        {
            log.debug( "Ignoring transaction at log index %d since already committed up to %d", commandIndex, lastCommittedIndex );
            return;
        }

        TransactionRepresentation tx;

        byte[] extraHeader = encodeLogIndexAsTxHeader( commandIndex );
        tx = ReplicatedTransactionFactory.extractTransactionRepresentation( replicatedTx, extraHeader );

        int currentTokenId = lockTokenStateMachine.currentToken().id();
        int txLockSessionId = tx.getLockSessionId();

        if ( currentTokenId != txLockSessionId && txLockSessionId != Locks.Client.NO_LOCK_SESSION_ID )
        {
            consumer.accept( Result.of( new TransactionFailureException(
                    LockSessionExpired, "The lock session in the cluster has changed: [current lock session id:%d, tx lock session id:%d]",
                    currentTokenId, txLockSessionId ) ) );
            return;
        }

        txQ.offer( new TxContext( consumer, tx ) );
    }
}
