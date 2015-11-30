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

import org.junit.Test;

import java.net.InetSocketAddress;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Set;
import java.util.TreeSet;
import java.util.UUID;
import java.util.concurrent.TimeUnit;

import org.neo4j.coreedge.server.AdvertisedSocketAddress;
import org.neo4j.coreedge.server.CoreMember;
import org.neo4j.coreedge.server.ListenSocketAddress;
import org.neo4j.function.ThrowingSupplier;
import org.neo4j.kernel.impl.util.JobScheduler;
import org.neo4j.kernel.impl.util.Neo4jJobScheduler;
import org.neo4j.logging.NullLogProvider;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.neo4j.helpers.collection.Iterables.count;
import static org.neo4j.helpers.collection.IteratorUtil.asSet;
import static org.neo4j.test.Assert.assertEventually;

public class SimpleDiscoveryServiceIT
{
    Set<SimpleDiscoveryService> makeDiscoveryServices( JobScheduler scheduler,
            int refreshIntervalMillis, int timeoutIntervalMillis, Set<Integer> basePorts )
    {
        UUID clusterUUID = UUID.randomUUID();

        Set<SimpleDiscoveryService> services = new HashSet<>();
        TreeSet<AdvertisedSocketAddress> fixedDiscoveryAddresses = new TreeSet<>();

        for ( int basePort : basePorts )
        {
            fixedDiscoveryAddresses.add(
                    new AdvertisedSocketAddress( new InetSocketAddress( "localhost", basePort ) ) );
        }

        for ( int basePort : basePorts )
        {
            AdvertisedSocketAddress discovery = new AdvertisedSocketAddress( new InetSocketAddress( "localhost", basePort++ ) );

            CoreMember member = new CoreMember(
                    new AdvertisedSocketAddress( new InetSocketAddress( "localhost", basePort++ ) ),
                    new AdvertisedSocketAddress( new InetSocketAddress( "localhost", basePort ) ) );

            services.add( new SimpleDiscoveryService( member, new ListenSocketAddress( discovery.socketAddress() ),
                    discovery, clusterUUID, fixedDiscoveryAddresses, NullLogProvider.getInstance(), scheduler,
                    refreshIntervalMillis, timeoutIntervalMillis, 3 ) );
        }

        return services;
    }

    @Test
    public void shouldDiscoverEachOther() throws Throwable
    {
        // given
        Neo4jJobScheduler scheduler = new Neo4jJobScheduler();
        scheduler.init();
        scheduler.start();

        Set<SimpleDiscoveryService> services = makeDiscoveryServices(
                scheduler, 100, 1000,
                asSet( 9000, 9010, 9020 ) );

        // when
        for ( SimpleDiscoveryService service : services )
        {
            service.start();
        }

        // then
        for ( SimpleDiscoveryService service : services )
        {
            ThrowingSupplier<Long,Exception> memberCount = () -> count( service.currentTopology().getMembers() );
            assertEventually( "discovered", memberCount, equalTo( 3L ), 20, TimeUnit.SECONDS );
        }

        for ( SimpleDiscoveryService service : services )
        {
            service.stop();
        }

        scheduler.stop();
    }

    @Test
    public void shouldForgetAboutGoneMember() throws Throwable
    {
        // given
        Neo4jJobScheduler scheduler = new Neo4jJobScheduler();
        scheduler.init();
        scheduler.start();

        Set<SimpleDiscoveryService> services = makeDiscoveryServices(
                scheduler, 100, 1000,
                asSet( 9000, 9001 ) );

        for ( SimpleDiscoveryService service : services )
        {
            service.start();
        }

        for ( SimpleDiscoveryService service : services )
        {
            assertEventually( "discovered", memberCount ( service ), equalTo( 2L ), 20, TimeUnit.SECONDS );
        }

        Iterator<SimpleDiscoveryService> iterator = services.iterator();

        // when
        SimpleDiscoveryService service = iterator.next();
        service.stop();

        // then
        service = iterator.next();
        assertEventually( "discovered", memberCount( service ), equalTo( 1L ), 20, TimeUnit.SECONDS );

        scheduler.stop();
    }

    ThrowingSupplier<Long,Exception> memberCount( SimpleDiscoveryService service )
    {
        return () -> Long.valueOf( service.currentTopology().getMembers().size() );
    }
}
