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
