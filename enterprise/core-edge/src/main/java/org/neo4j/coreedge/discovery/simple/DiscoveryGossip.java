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

import java.util.HashMap;
import java.util.Map;
import java.util.UUID;

import org.neo4j.coreedge.server.CoreMember;

/**
 * Used to package all the information a particular instance has discovered
 * about other instances.
 */
class DiscoveryGossip
{
    private static CoreMember.CoreMemberMarshal memberMarshal = new CoreMember.CoreMemberMarshal();

    private UUID clusterUUID;
    private Map<CoreMember,DiscoveryInfo> infoMap;

    DiscoveryGossip( UUID clusterUUID, Map<CoreMember,DiscoveryInfo> infoMap )
    {
        this.clusterUUID = clusterUUID;
        this.infoMap = infoMap;
    }

    public UUID clusterUUID()
    {
        return clusterUUID;
    }

    public Map<CoreMember,DiscoveryInfo> info()
    {
        return infoMap;
    }

    public void serialize( ByteBuf buffer)
    {
        buffer.writeLong( clusterUUID.getMostSignificantBits() );
        buffer.writeLong( clusterUUID.getLeastSignificantBits() );

        buffer.writeInt( infoMap.size() );
        for ( CoreMember member : infoMap.keySet() )
        {
            DiscoveryInfo info = infoMap.get( member );
            memberMarshal.marshal( member, buffer );
            DiscoveryInfo.serialize( buffer, info );
        }
    }

    public static DiscoveryGossip deserialize( ByteBuf buffer )
    {
        long uuidMSB = buffer.readLong();
        long uuidLSB = buffer.readLong();
        UUID clusterUUID = new UUID( uuidMSB, uuidLSB );

        HashMap<CoreMember,DiscoveryInfo> infos = new HashMap<>();
        int infoCount = buffer.readInt();
        while( infoCount-->0 )
        {
            CoreMember member = memberMarshal.unmarshal( buffer );
            DiscoveryInfo discoveryInfo = DiscoveryInfo.deserialize( buffer );
            infos.put( member, discoveryInfo );
        }

        return new DiscoveryGossip( clusterUUID, infos );
    }

    @Override
    public String toString()
    {
        return "DiscoveryGossip{" +
               "clusterUUID=" + clusterUUID +
               ", infoMap=" + infoMap +
               '}';
    }
}
