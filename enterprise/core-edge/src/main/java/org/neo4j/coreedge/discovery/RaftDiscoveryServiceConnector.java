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
package org.neo4j.coreedge.discovery;

import org.neo4j.coreedge.raft.log.RaftLogCompactedException;
import org.neo4j.coreedge.server.CoreMember;
import org.neo4j.coreedge.raft.RaftInstance;
import org.neo4j.coreedge.raft.membership.CoreMemberSet;
import org.neo4j.kernel.lifecycle.LifecycleAdapter;
import org.neo4j.logging.Log;
import org.neo4j.logging.LogProvider;

public class RaftDiscoveryServiceConnector extends LifecycleAdapter implements CoreDiscoveryService.Listener
{
    private final CoreDiscoveryService discoveryService;
    private final RaftInstance<CoreMember> raftInstance;
    private boolean isBootstrapped;
    private final Log log;

    public RaftDiscoveryServiceConnector( CoreDiscoveryService discoveryService, RaftInstance<CoreMember> raftInstance, LogProvider logProvider, boolean isBootstrapped )
    {
        this.discoveryService = discoveryService;
        this.raftInstance = raftInstance;
        this.isBootstrapped = isBootstrapped;
        this.log = logProvider.getLog( getClass() );
    }

    @Override
    public void start() throws BootstrapException
    {
        discoveryService.addMembershipListener( this );
        onTopologyChange( discoveryService.currentTopology() );
    }

    @Override
    public void stop()
    {
        discoveryService.removeMembershipListener( this );
    }

    @Override
    public void onTopologyChange( ClusterTopology clusterTopology )
    {
        if ( !isBootstrapped && clusterTopology.canBootstrapLocally() )
        {
            try
            {
                raftInstance.bootstrapWithInitialMembers( new CoreMemberSet( clusterTopology.getMembers() ) );
                discoveryService.wasLocallyBootstrapped();
                isBootstrapped = true;
            }
            catch ( BootstrapException | RaftLogCompactedException e )
            {
                log.error( "Bootstrap failed", e );
            }
        }

        raftInstance.setTargetMembershipSet( clusterTopology.getMembers() );
    }
}
