/*
 * Copyright (c) 2002-2018 "Neo Technology,"
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
package org.neo4j.causalclustering.discovery;

import org.neo4j.kernel.lifecycle.LifecycleAdapter;

public abstract class AbstractTopologyService extends LifecycleAdapter implements TopologyService
{

    @Override
    public CoreTopology localCoreServers()
    {
        //TODO: The filterTopologyByDb method is not going to return a new ClusterId for the filtered topology.
        // Even though the map for this exists in hazelcast. Perhaps we need to do the filtering in the Concrete
        // *CoreTopologyService classes and make lookups accordingly.
        return allCoreServers().filterTopologyByDb( localDBName() );
    }

    @Override
    public ReadReplicaTopology localReadReplicas()
    {
        return allReadReplicas().filterTopologyByDb( localDBName() );
    }
}
