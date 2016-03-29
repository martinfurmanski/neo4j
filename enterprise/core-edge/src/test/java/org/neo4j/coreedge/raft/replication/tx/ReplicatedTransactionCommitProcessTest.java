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

import org.neo4j.coreedge.raft.replication.Replicator;
import org.neo4j.kernel.impl.api.TransactionToApply;
import org.neo4j.kernel.impl.transaction.TransactionRepresentation;

import static org.mockito.Matchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.neo4j.kernel.impl.transaction.tracing.CommitEvent.NULL;
import static org.neo4j.storageengine.api.TransactionApplicationMode.INTERNAL;

@SuppressWarnings("unchecked")
public class ReplicatedTransactionCommitProcessTest
{
    // TODO: Test something?

    private TransactionToApply tx()
    {
        TransactionRepresentation tx = mock( TransactionRepresentation.class );
        when( tx.additionalHeader() ).thenReturn( new byte[]{} );
        return new TransactionToApply( tx );
    }
}
