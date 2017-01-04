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
package org.neo4j.causalclustering.core.state.machines.locks;

import org.neo4j.causalclustering.core.state.machines.locks.net.RemoteLocks;
import org.neo4j.causalclustering.core.state.machines.locks.token.LockToken;
import org.neo4j.causalclustering.identity.MemberId;

/**
 * This represents the global context within which the locks are to be taken.
 *
 * It essentially binds to the lock manager which was valid at a
 * particular point in time and carries the associated global lock
 * token.
 *
 * The context also gives access to the associated RPC implementation.
 */
public class LockingContext
{
    private final LockToken token;
    private final RemoteLocks rpc;
    private final boolean isLocal;

    LockingContext( LockToken token, RemoteLocks rpc, boolean isLocal )
    {
        this.token = token;
        this.rpc = rpc;
        this.isLocal = isLocal;
    }

    public LockToken token()
    {
        return token;
    }

    public MemberId owner()
    {
        return (MemberId) token.owner();
    }

    public RemoteLocks rpc()
    {
        return rpc;
    }

    public boolean isLocal()
    {
        return isLocal;
    }

    @Override
    public String toString()
    {
        return "LockingContext{" +
               "token=" + token +
               ", rpc=" + rpc +
               ", isLocal=" + isLocal +
               '}';
    }
}
