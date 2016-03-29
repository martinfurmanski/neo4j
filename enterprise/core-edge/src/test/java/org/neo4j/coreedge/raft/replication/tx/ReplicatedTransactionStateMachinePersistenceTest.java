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

import org.junit.Test;

import java.util.Collections;

import org.neo4j.coreedge.server.RaftTestMember;
import org.neo4j.coreedge.server.core.RecoverTransactionLogState;
import org.neo4j.coreedge.server.core.locks.ReplicatedLockTokenStateMachine;
import org.neo4j.graphdb.TransactionFailureException;
import org.neo4j.kernel.impl.api.TransactionCommitProcess;
import org.neo4j.kernel.impl.transaction.TransactionRepresentation;
import org.neo4j.kernel.impl.transaction.log.PhysicalTransactionRepresentation;
import org.neo4j.logging.NullLogProvider;

import static org.junit.Assert.fail;
import static org.mockito.Matchers.any;
import static org.mockito.Mockito.RETURNS_MOCKS;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.reset;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class ReplicatedTransactionStateMachinePersistenceTest
{
    @Test
    public void shouldNotRejectUncommittedTransactionsAfterCrashEvenIfSessionTrackerSaysSo() throws Exception
    {
        // given
        TransactionCommitProcess commitProcess = mock( TransactionCommitProcess.class );
        when( commitProcess.commit( any(), any(), any() ) ).thenThrow( new TransactionFailureException( "testing" ) )
                .thenReturn( 123L );

        RecoverTransactionLogState recoverTransactionLogState = mock( RecoverTransactionLogState.class );
        when( recoverTransactionLogState.findLastAppliedIndex() ).thenReturn( 99L );

        ReplicatedTransactionStateMachine<RaftTestMember> stateMachine = stateMachine( commitProcess,
                recoverTransactionLogState );

        ReplicatedTransaction rtx = replicatedTx();

        // when
        try
        {
            stateMachine.applyCommand( rtx, 100 );
            fail( "test design throws exception here" );
        }
        catch ( TransactionFailureException thrownByTestDesign )
        {
            // expected
        }
        reset( commitProcess ); // ignore all previous interactions, we care what happens from now on
        stateMachine.setLastCommittedIndex( 99 );
        stateMachine.applyCommand( rtx, 100 );

        // then
        verify( commitProcess, times( 1 ) ).commit( any(), any(), any() );
    }

    public ReplicatedTransactionStateMachine<RaftTestMember> stateMachine(
            TransactionCommitProcess commitProcess,
            RecoverTransactionLogState recoverTransactionLogState )
    {
        return new ReplicatedTransactionStateMachine<RaftTestMember>(
                commitProcess,
                mock( ReplicatedLockTokenStateMachine.class, RETURNS_MOCKS ),
                NullLogProvider.getInstance(),
                recoverTransactionLogState );
    }

    private ReplicatedTransaction replicatedTx() throws java.io.IOException
    {
        TransactionRepresentation tx = new PhysicalTransactionRepresentation( Collections.emptySet() );
        return ReplicatedTransactionFactory.createImmutableReplicatedTransaction( tx );
    }
}
