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

import org.neo4j.coreedge.raft.log.RaftLog;
import org.neo4j.coreedge.raft.replication.token.ReplicatedLabelTokenHolder;
import org.neo4j.coreedge.raft.replication.token.ReplicatedPropertyKeyTokenHolder;
import org.neo4j.coreedge.raft.replication.token.ReplicatedRelationshipTypeTokenHolder;
import org.neo4j.coreedge.raft.replication.token.ReplicatedTokenHolder;
import org.neo4j.coreedge.raft.replication.tx.ReplicatedTransactionStateMachine;
import org.neo4j.kernel.NeoStoreDataSource;
import org.neo4j.kernel.impl.transaction.CommittedTransactionRepresentation;
import org.neo4j.kernel.impl.transaction.log.IOCursor;
import org.neo4j.kernel.impl.transaction.log.LogicalTransactionStore;
import org.neo4j.kernel.impl.util.Dependencies;
import org.neo4j.kernel.lifecycle.LifecycleAdapter;

import static org.neo4j.coreedge.raft.replication.tx.ReplicatedTransactionStateMachine.bytesToLong;

public class RaftLogReplay extends LifecycleAdapter
{
    private final RaftLog raftLog;
    private final Dependencies dependencies;

    public RaftLogReplay( RaftLog raftLog, Dependencies dependencies )
    {
        this.raftLog = raftLog;
        this.dependencies = dependencies;
    }

    @Override
    public void start() throws Throwable
    {
        System.out.println("NOW I SHALL PLAY YOU THE SONG OF MY PEOPLE");
        long lastCommittedIndex;
        long lastTxId = dependencies.resolveDependency( NeoStoreDataSource.class ).getNeoStores().getMetaDataStore().getLastCommittedTransactionId();

        if ( lastTxId == 1 )
        {
            lastCommittedIndex = -1;
        }
        else
        {
            IOCursor<CommittedTransactionRepresentation> transactions = null;
            byte[] lastHeaderFound;
            try
            {
                transactions = dependencies.resolveDependency(
                        LogicalTransactionStore.class ).getTransactions( lastTxId );
                lastHeaderFound = null;
                while ( transactions.next() )
                {
                    CommittedTransactionRepresentation committedTransactionRepresentation = transactions.get();
                    lastHeaderFound = committedTransactionRepresentation.getStartEntry().getAdditionalHeader();
                }
            }
            catch ( Exception e )
            {
                throw new RuntimeException( e );
            }
            lastCommittedIndex = bytesToLong( lastHeaderFound );
        }

        System.out.println("The last index has been determined to be " + lastCommittedIndex );

        ReplicatedTransactionStateMachine replicatedTransactionStateMachine = dependencies.resolveDependency(
                ReplicatedTransactionStateMachine.class );
        ReplicatedTokenHolder relTypeTokenHolder = dependencies.resolveDependency( ReplicatedRelationshipTypeTokenHolder
                .class );
        ReplicatedTokenHolder labelTokenHolder = dependencies.resolveDependency( ReplicatedLabelTokenHolder.class );
        ReplicatedTokenHolder propKeyTokenHolder = dependencies.resolveDependency( ReplicatedPropertyKeyTokenHolder.class );

        replicatedTransactionStateMachine.setRecoveryMode( lastCommittedIndex );
        relTypeTokenHolder.setRecoveryMode( lastCommittedIndex );
        labelTokenHolder.setRecoveryMode( lastCommittedIndex );
        propKeyTokenHolder.setRecoveryMode( lastCommittedIndex );

        long start = System.currentTimeMillis();
        raftLog.replay();
        System.out.println("Replay done, took " + (System.currentTimeMillis() - start) + " ms");

        replicatedTransactionStateMachine.unsetRecoveryMode();
        relTypeTokenHolder.unsetRecoveryMode();
        labelTokenHolder.unsetRecoveryMode();
        propKeyTokenHolder.unsetRecoveryMode();
    }
}
