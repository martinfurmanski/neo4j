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
package org.neo4j.coreedge.server.core;

import org.neo4j.coreedge.raft.locks.CoreServiceAssignment;
import org.neo4j.coreedge.raft.replication.ReplicatedContent;
import org.neo4j.coreedge.raft.replication.Replicator;
import org.neo4j.graphdb.TransientTransactionFailureException;
import org.neo4j.kernel.api.exceptions.TransactionFailureException;
import org.neo4j.kernel.impl.api.TransactionApplicationMode;
import org.neo4j.kernel.impl.api.TransactionCommitProcess;
import org.neo4j.kernel.impl.api.TransactionToApply;
import org.neo4j.kernel.impl.transaction.tracing.CommitEvent;

/** This commit process rejects transactions started during old leader reigns. This is necessary
 *  because we use distributed unsynchronized lock managers. */
public class StaleTransactionRejectingCommitProcess implements TransactionCommitProcess, Replicator.ReplicatedContentListener
{
    private final TransactionCommitProcess delegate;

    private long lastCommittedTxId; // Maintains the last committed tx id, used to set the next field
    private long lastTxIdForPreviousAssignment; // Maintains the last txid committed under the previous service assignment

    public StaleTransactionRejectingCommitProcess( TransactionCommitProcess delegate )
    {
        this.delegate = delegate;
    }

    @Override
    public long commit( TransactionToApply tx, CommitEvent commitEvent, TransactionApplicationMode mode ) throws TransactionFailureException
    {
        if ( tx.transactionRepresentation().getLatestCommittedTxWhenStarted() >= lastTxIdForPreviousAssignment )
        {
            lastCommittedTxId = delegate.commit( tx , commitEvent, mode );
            return lastCommittedTxId;
        }
        else
        {
            // This means the transaction started before the last transaction for the previous term. Reject.

            throw new TransientTransactionFailureException(
                "Attempt to commit transaction that was started on a different leader term. " +
                "Please retry the transaction." );
        }
    }

    @Override
    public void onReplicated( ReplicatedContent content, long index )
    {
        if ( content instanceof CoreServiceAssignment )
        {
            // This essentially signifies a leader switch. We should properly name the content class
            lastTxIdForPreviousAssignment = lastCommittedTxId;
        }
    }
}
