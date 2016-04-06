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

import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;
import java.util.SortedMap;
import java.util.TreeMap;
import java.util.TreeSet;
import java.util.UUID;

import org.neo4j.coreedge.discovery.ClusterTopology;
import org.neo4j.coreedge.discovery.CoreDiscoveryService;
import org.neo4j.coreedge.server.AdvertisedSocketAddress;
import org.neo4j.coreedge.server.CoreMember;
import org.neo4j.coreedge.server.ListenSocketAddress;
import org.neo4j.kernel.impl.util.JobScheduler;
import org.neo4j.logging.Log;
import org.neo4j.logging.LogProvider;

import static java.util.concurrent.TimeUnit.MILLISECONDS;

// TODO: Edge machine should be able to discover a core machine.
// TODO: Broadcast leave on shutdown. Perhaps simply gossip with zero TTL.

/**
 * A gossiping discovery service. Gossips information about the cluster to all
 * configured contact points on a consistent basis.
 *
 *  - each piece of information about a member originates from that member
 *    but will be gossiped on to other members
 *
 *  - each piece of information about a member has a TTL value associated
 *    with it
 *
 *  The TTL value is set to its initial value by the original member when
 *  it informs other members that it is still alive. Members receiving
 *  information always update the TTL to the highest known, meaning
 *  that the local value is compared to the received value. The member
 *  decreases the local TTL for a member over time, and only a fresh
 *  piece of information from the origin can refresh it.
 */
public class SimpleDiscoveryService implements CoreDiscoveryService
{
    private final CoreMember myself;
    private final SimpleDiscoveryServer server;
    private final UUID clusterUUID;

    private final Set<AdvertisedSocketAddress> fixedDiscoveryAddresses;
    private final Set<AdvertisedSocketAddress> foundDiscoveryAddresses;
    private final SortedMap<CoreMember,DiscoveryInfo> infoMap = new TreeMap<>();

    private Map<AdvertisedSocketAddress,SimpleDiscoveryClient> clientMap = new HashMap<>();

    private final HashSet<Listener> membershipListeners = new HashSet<>();
    private final Log log;

    private final LogProvider logProvider;
    private final AdvertisedSocketAddress myDiscoveryAddress;
    private final JobScheduler scheduler;
    private JobScheduler.JobHandle jobHandle;

    private final int refreshIntervalMillis;
    private final int timeoutIntervalMillis;
    private final int expectedCoreClusterSize;

    public SimpleDiscoveryService( CoreMember myself, ListenSocketAddress listenAddress, AdvertisedSocketAddress myDiscoveryAddress,
            UUID clusterUUID, Set<AdvertisedSocketAddress> fixedDiscoveryAddresses,
            LogProvider logProvider, JobScheduler scheduler, int refreshIntervalMillis, int timeoutIntervalMillis,
            int expectedCoreClusterSize )
    {
        this.myself = myself;
        this.myDiscoveryAddress = myDiscoveryAddress;
        this.scheduler = scheduler;
        this.refreshIntervalMillis = refreshIntervalMillis;
        this.timeoutIntervalMillis = timeoutIntervalMillis;
        this.expectedCoreClusterSize = expectedCoreClusterSize;
        this.server = new SimpleDiscoveryServer( listenAddress, this, logProvider );

        this.clusterUUID = clusterUUID;
        this.fixedDiscoveryAddresses = new TreeSet<>( fixedDiscoveryAddresses );
        this.foundDiscoveryAddresses = new TreeSet<>();

        this.log = logProvider.getLog( getClass() );
        this.logProvider = logProvider;
        this.infoMap.put( myself, new DiscoveryInfo( myself, myDiscoveryAddress, timeoutIntervalMillis, false ) );
    }

    @Override
    public synchronized void addMembershipListener( Listener listener )
    {
        membershipListeners.add( listener );
    }

    @Override
    public synchronized void removeMembershipListener( Listener listener )
    {
        membershipListeners.remove( listener );
    }

    @Override
    public synchronized void wasLocallyBootstrapped()
    {
        this.infoMap.put( myself, new DiscoveryInfo( myself, myDiscoveryAddress, timeoutIntervalMillis, true ) );
        broadcastDiscoveryInfo();
    }

    @Override
    public synchronized ClusterTopology currentTopology()
    {
        return new SimpleClusterTopology( infoMap, canBootstrap() );
    }

    private boolean canBootstrap()
    {
        if ( infoMap.keySet().size() < expectedCoreClusterSize )
        {
            return false;
        }

        for ( DiscoveryInfo discoveryInfo : infoMap.values() )
        {
            if( discoveryInfo.isBootstrapped() )
            {
                return false;
            }
        }

        return true;
    }

    void processGossip( DiscoveryGossip gossip )
    {
        if( !gossip.clusterUUID().equals( clusterUUID ) )
        {
            return;
        }

        checkForPortConflicts( gossip );

        boolean changed = updateInfo( gossip );
        if ( changed )
        {
            notifyListeners();
        }
    }

    private void checkForPortConflicts( DiscoveryGossip gossip )
    {
        // TODO Scan for any port conflicts and decide on strategy for handling. Perhaps
        // TODO logging is sufficient? There is no unique UUID for members so equality is
        // TODO checked by comparing all advertised addresses. It is not easy in such a
        // TODO scheme to know if a conflict is due to reconfiguration or misconfiguration.
    }

    private synchronized boolean updateInfo( DiscoveryGossip gossip )
    {
        boolean changed = false;
        for ( CoreMember coreMember : gossip.info().keySet() )
        {
            if( coreMember.equals( myself ) )
                continue;

            if ( updateInfo( coreMember, gossip.info().get( coreMember ) ) )
            {
                changed = true;
            }
        }
        return changed;
    }

    private boolean updateInfo( CoreMember member, DiscoveryInfo gossipInfo )
    {
        DiscoveryInfo localInfo = infoMap.get( member );

        if ( localInfo == null )
        {
            if ( !fixedDiscoveryAddresses.contains( gossipInfo.discoveryAddress() ) )
            {
                foundDiscoveryAddresses.add( gossipInfo.discoveryAddress() );
            }

            log.info( "Discovered: " + member );

            infoMap.put( member, gossipInfo );
            return true;
        }
        else if( localInfo.timeToLive() < gossipInfo.timeToLive() )
        {
            // NOTE: This will not catch changes to advertised addresses.
            infoMap.put( member, gossipInfo );
        }
        return false;
    }

    private void notifyListeners()
    {
        ClusterTopology clusterTopology = currentTopology();

        for ( Listener listener : membershipListeners )
        {
            listener.onTopologyChange( clusterTopology );
        }
    }

    private void send( AdvertisedSocketAddress address, DiscoveryGossip discoveryGossip )
    {
        SimpleDiscoveryClient client = clientMap.get( address );
        if ( client == null )
        {
            client = new SimpleDiscoveryClient( address, logProvider, discoveryGossip );
            clientMap.put( address, client );
        }

        client.write( discoveryGossip );
    }

    private void recurringWork()
    {
        try
        {
            boolean changed = decayTTL();

            if ( changed )
            {
                notifyListeners();
            }

            broadcastDiscoveryInfo();
        }
        catch( Throwable e )
        {
            log.error( "Error during recurring work of discovery service", e );
        }
    }

    private synchronized boolean decayTTL()
    {
        boolean changed = false;

        Iterator<Map.Entry<CoreMember,DiscoveryInfo>> itr = infoMap.entrySet().iterator();

        while( itr.hasNext() )
        {
            Map.Entry<CoreMember,DiscoveryInfo> entry = itr.next();

            CoreMember member = entry.getKey();

            if( member.equals( myself ) )
                continue;

            DiscoveryInfo info = entry.getValue();
            info.decreaseTimeToLive( refreshIntervalMillis );
            if ( info.timeToLive() <= 0 )
            {
                itr.remove();
                log.info( "Undiscovered: " + member );

                SimpleDiscoveryClient client = clientMap.remove( info.discoveryAddress() );
                if ( client != null )
                {
                    client.destroy();
                }
                foundDiscoveryAddresses.remove( info.discoveryAddress() );
                changed = true;
            }
        }

        return changed;
    }

    private synchronized void broadcastDiscoveryInfo()
    {
        DiscoveryGossip discoveryGossip = new DiscoveryGossip( clusterUUID, infoMap );

        for ( AdvertisedSocketAddress address : fixedDiscoveryAddresses )
        {
            send( address, discoveryGossip );
        }

        for ( AdvertisedSocketAddress address : foundDiscoveryAddresses )
        {
            if( fixedDiscoveryAddresses.contains( address ) )
                continue;

            send( address, discoveryGossip );
        }
    }

    @Override
    public synchronized void start() throws Throwable
    {
        server.start();
        jobHandle = scheduler.scheduleRecurring( new JobScheduler.Group( "discovery-recurring", JobScheduler.SchedulingStrategy.POOLED ),
                this::recurringWork, 0, refreshIntervalMillis, MILLISECONDS );
    }

    @Override
    public synchronized void stop() throws Throwable
    {
        jobHandle.cancel( true );
        server.stop();
    }

    @Override
    public void init() throws Throwable
    {
    }

    @Override
    public void shutdown() throws Throwable
    {
    }
}
