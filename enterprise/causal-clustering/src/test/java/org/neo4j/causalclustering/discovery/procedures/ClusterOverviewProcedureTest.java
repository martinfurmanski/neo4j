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
package org.neo4j.causalclustering.discovery.procedures;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import org.hamcrest.collection.IsIterableContainingInOrder;
import org.junit.Test;

import org.neo4j.causalclustering.core.consensus.LeaderLocator;
import org.neo4j.causalclustering.discovery.CoreAddresses;
import org.neo4j.causalclustering.discovery.CoreTopology;
import org.neo4j.causalclustering.discovery.CoreTopologyService;
import org.neo4j.causalclustering.discovery.ReadReplicaAddresses;
import org.neo4j.causalclustering.discovery.ReadReplicaTopology;
import org.neo4j.causalclustering.identity.MemberId;
import org.neo4j.logging.NullLogProvider;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import static org.neo4j.causalclustering.discovery.TestTopology.addressesForReadReplicas;
import static org.neo4j.causalclustering.discovery.TestTopology.adressesForCore;
import static org.neo4j.helpers.collection.Iterators.asList;

public class ClusterOverviewProcedureTest
{
    @Test
    public void shouldProvideOverviewOfCoreServersAndReadReplicas() throws Exception
    {
        // given
        final CoreTopologyService topologyService = mock( CoreTopologyService.class );

        Map<MemberId,CoreAddresses> coreMembers = new HashMap<>();
        MemberId theLeader = new MemberId( UUID.randomUUID() );
        MemberId follower1 = new MemberId( UUID.randomUUID() );
        MemberId follower2 = new MemberId( UUID.randomUUID() );

        coreMembers.put( theLeader, adressesForCore( 0 ) );
        coreMembers.put( follower1, adressesForCore( 1 ) );
        coreMembers.put( follower2, adressesForCore( 2 ) );

        Map<MemberId,ReadReplicaAddresses> readReplicas = addressesForReadReplicas( 4, 5 );

        when( topologyService.coreServers() ).thenReturn( new CoreTopology( null, false, coreMembers ) );
        when( topologyService.readReplicas() ).thenReturn( new ReadReplicaTopology( readReplicas ) );

        LeaderLocator leaderLocator = mock( LeaderLocator.class );
        when( leaderLocator.getLeader() ).thenReturn( theLeader );

        ClusterOverviewProcedure procedure = new ClusterOverviewProcedure( topologyService, leaderLocator,
                NullLogProvider.getInstance() );

        // when
        final List<Object[]> members = asList( procedure.apply( null, new Object[0] ) );

        // then
        assertThat( members, IsIterableContainingInOrder.contains(
                new Object[]{theLeader.getUuid().toString(), new String[] {"bolt://localhost:5000"}, "LEADER"},
                new Object[]{follower1.getUuid().toString(), new String[] {"bolt://localhost:5001"}, "FOLLOWER"},
                new Object[]{follower2.getUuid().toString(), new String[] {"bolt://localhost:5002"}, "FOLLOWER"},
                new Object[]{"00000000-0000-0000-0000-000000000000", new String[] {"bolt://localhost:7004"}, "READ_REPLICA"},
                new Object[]{"00000000-0000-0000-0000-000000000000", new String[] {"bolt://localhost:7005"}, "READ_REPLICA"}
        ) );
    }
}
