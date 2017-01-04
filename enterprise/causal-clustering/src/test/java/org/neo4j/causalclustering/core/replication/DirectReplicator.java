/*
 * Copyright (c) 2002-2017 "Neo Technology,"
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
package org.neo4j.causalclustering.core.replication;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import org.neo4j.causalclustering.core.replication.session.OperationContext;
import org.neo4j.causalclustering.core.state.machines.StateMachine;

public class DirectReplicator<Command extends ReplicatedContent> implements Replicator
{
    private final StateMachine<Command> stateMachine;
    private long commandIndex = 0;

    public DirectReplicator( StateMachine<Command> stateMachine )
    {
        this.stateMachine = stateMachine;
    }

    @Override
    public Future<Object> replicate( ReplicatedContent content, boolean trackResult, OperationContext context ) throws InterruptedException
    {
        return replicate( content, trackResult ); // TODO
    }

    @Override
    public synchronized Future<Object> replicate( ReplicatedContent content, boolean trackResult )
    {
        CompletableFuture<Object> futureResult = new CompletableFuture<>();
        stateMachine.applyCommand( (Command) content, commandIndex++, result -> {
            if ( trackResult )
            {
                result.apply( futureResult );
            }
        } );

        return futureResult;
    }

    @Override
    public void await( long dataVersion, long timeoutMs, TimeUnit timeUnit ) throws TimeoutException
    {
        throw new UnsupportedOperationException();
    }
}
