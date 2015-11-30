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
package org.neo4j.coreedge.discovery.simple;

import java.util.Set;
import java.util.SortedMap;
import java.util.TreeSet;

import org.neo4j.coreedge.discovery.ClusterTopology;
import org.neo4j.coreedge.server.AdvertisedSocketAddress;
import org.neo4j.coreedge.server.CoreMember;

class SimpleClusterTopology implements ClusterTopology
{
    final Set<CoreMember> members;
    final AdvertisedSocketAddress firstTransactionServer;
    private final boolean shouldBootstrap;

    public SimpleClusterTopology( SortedMap<CoreMember,DiscoveryInfo> infoMap, boolean shouldBootstrap )
    {
        this.members = new TreeSet<>( infoMap.keySet() );
        this.firstTransactionServer = infoMap.firstKey().getCoreAddress();
        this.shouldBootstrap = shouldBootstrap;
    }

    @Override
    public AdvertisedSocketAddress firstTransactionServer()
    {
        return firstTransactionServer;
    }

    @Override
    public int getNumberOfEdgeServers()
    {
        return 0; // TODO: Edge machines?
    }

    @Override
    public int getNumberOfCoreServers()
    {
        return members.size();
    }

    @Override
    public Set<CoreMember> getMembers()
    {
        return members;
    }

    @Override
    public boolean bootstrappable()
    {
        return shouldBootstrap;
    }
}
