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

import io.netty.buffer.ByteBuf;

import org.neo4j.coreedge.server.AdvertisedSocketAddress;
import org.neo4j.coreedge.server.CoreMember;

import static java.lang.String.format;

/**
 * Information about a CoreMember. It's discovery address and time that this
 * discovery should be kept alive from the point of receiving this information
 * from the origin.
 */
class DiscoveryInfo
{
    private static CoreMember.CoreMemberMarshal memberMarshal = new CoreMember.CoreMemberMarshal();
    private static AdvertisedSocketAddress.AdvertisedSocketAddressByteBufferMarshal addressMarshal = new AdvertisedSocketAddress.AdvertisedSocketAddressByteBufferMarshal();

    private final CoreMember member;
    private final AdvertisedSocketAddress discoveryAddress;
    private long timeToLiveMillis;
    private boolean isBootstrapped;

    DiscoveryInfo( CoreMember member, AdvertisedSocketAddress discoveryAddress, long timeToLiveMillis, boolean isBootstrapped )
    {
        this.member = member;
        this.discoveryAddress = discoveryAddress;
        this.timeToLiveMillis = timeToLiveMillis;
        this.isBootstrapped = isBootstrapped;
    }

    AdvertisedSocketAddress discoveryAddress()
    {
        return discoveryAddress;
    }

    long timeToLive()
    {
        return timeToLiveMillis;
    }

    boolean isBootstrapped()
    {
        return isBootstrapped;
    }

    public static void serialize( ByteBuf buffer, DiscoveryInfo discoveryInfo )
    {
        memberMarshal.marshal( discoveryInfo.member, buffer );
        addressMarshal.marshal( discoveryInfo.discoveryAddress, buffer );

        buffer.writeLong( discoveryInfo.timeToLiveMillis );
        buffer.writeBoolean( discoveryInfo.isBootstrapped );
    }

    static DiscoveryInfo deserialize( ByteBuf buffer )
    {
        CoreMember origin = memberMarshal.unmarshal( buffer );
        AdvertisedSocketAddress discoveryAddress = addressMarshal.unmarshal( buffer );

        long timeToLiveMillis = buffer.readLong();
        boolean isBootstrapped = buffer.readBoolean();

        return new DiscoveryInfo( origin, discoveryAddress, timeToLiveMillis, isBootstrapped );
    }

    void decreaseTimeToLive( int gossipIntervalMillis )
    {
        this.timeToLiveMillis -= gossipIntervalMillis;
    }

    @Override
    public String toString()
    {
        return format( "DiscoveryInfo{member=%s, discoveryAddress=%s, timeToLiveMillis=%d}", member, discoveryAddress, timeToLiveMillis );
    }
}
