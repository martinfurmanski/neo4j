package org.neo4j.coreedge.discovery.simple;

import java.util.HashSet;
import java.util.Set;
import java.util.UUID;

import org.neo4j.coreedge.discovery.*;
import org.neo4j.coreedge.discovery.CoreDiscoveryService;
import org.neo4j.coreedge.server.AdvertisedSocketAddress;
import org.neo4j.coreedge.server.CoreEdgeClusterSettings;
import org.neo4j.coreedge.server.CoreMember;
import org.neo4j.coreedge.server.ListenSocketAddress;
import org.neo4j.kernel.configuration.Config;
import org.neo4j.kernel.impl.util.Neo4jJobScheduler;
import org.neo4j.logging.LogProvider;
import org.neo4j.logging.NullLogProvider;

public class SimpleDiscoveryServiceFactory implements DiscoveryServiceFactory
{
    @Override
    public CoreDiscoveryService coreDiscoveryService( Config config )
    {
        UUID clusterUUID = new UUID( 0, 0 );
        LogProvider logProvider = NullLogProvider.getInstance();
        Neo4jJobScheduler jobScheduler = new Neo4jJobScheduler();
        jobScheduler.init();
        try
        {
            jobScheduler.start();
        }
        catch ( Throwable throwable )
        {
            throwable.printStackTrace();
        }

        AdvertisedSocketAddress coreAddress = config.get( CoreEdgeClusterSettings.transaction_advertised_address );
        AdvertisedSocketAddress raftAddress = config.get( CoreEdgeClusterSettings.raft_advertised_address );

        ListenSocketAddress listenAddress = config.get( CoreEdgeClusterSettings.cluster_listen_address );

        CoreMember myself = new CoreMember( coreAddress, raftAddress );

        Set<AdvertisedSocketAddress> fixedDiscoveryAddresses = new HashSet<>();
        for ( AdvertisedSocketAddress address : config.get( CoreEdgeClusterSettings.initial_core_cluster_members ) )
        {
            fixedDiscoveryAddresses.add( address );
        }

        int refreshIntervalMillis = 5000;
        int timeoutIntervalMillis = 60000;

        AdvertisedSocketAddress myDiscoveryAddress = new AdvertisedSocketAddress( listenAddress.socketAddress() );

        int bootstrapSize = 3;
        return new SimpleDiscoveryService( myself, listenAddress, myDiscoveryAddress, clusterUUID, fixedDiscoveryAddresses, logProvider, jobScheduler,
                refreshIntervalMillis, timeoutIntervalMillis, bootstrapSize );
    }

    @Override
    public EdgeDiscoveryService edgeDiscoveryService( Config config )
    {
        // TODO
        return null;
    }
}
