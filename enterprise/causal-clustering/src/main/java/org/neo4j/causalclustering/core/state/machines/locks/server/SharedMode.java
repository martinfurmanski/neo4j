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
package org.neo4j.causalclustering.core.state.machines.locks.server;

import org.neo4j.kernel.impl.locking.Locks;
import org.neo4j.kernel.impl.locking.ResourceTypes;

public class SharedMode extends Operations
{
    SharedMode( Locks.Client client )
    {
        super( client );
    }

    @Override
    public void acquire( ResourceTypes type, long[] ids )
    {
        client.acquireShared( type, ids );
    }

    @Override
    public boolean tryAcquire( ResourceTypes type, long id )
    {
        return client.trySharedLock( type, id );
    }

    @Override
    public void release( ResourceTypes type, long id )
    {
        client.releaseShared( type, id );
    }
}
