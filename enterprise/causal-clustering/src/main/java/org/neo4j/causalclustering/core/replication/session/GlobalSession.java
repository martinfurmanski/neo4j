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
package org.neo4j.causalclustering.core.replication.session;

import java.io.IOException;
import java.util.UUID;

import org.neo4j.causalclustering.core.state.storage.SafeChannelMarshal;
import org.neo4j.causalclustering.identity.MemberId;
import org.neo4j.causalclustering.messaging.EndOfStreamException;
import org.neo4j.storageengine.api.ReadableChannel;
import org.neo4j.storageengine.api.WritableChannel;

public class GlobalSession
{
    private final GlobalSessionId globalId;
    private final LocalOperationId operationId;

    GlobalSession( GlobalSessionId globalId, LocalOperationId operationId )
    {
        this.globalId = globalId;
        this.operationId = operationId;
    }

    public GlobalSessionId globalId()
    {
        return globalId;
    }

    public LocalOperationId operationId()
    {
        return operationId;
    }

    public static class Marshal extends SafeChannelMarshal<GlobalSession>
    {
        public static final GlobalSession.Marshal INSTANCE = new GlobalSession.Marshal();

        @Override
        public void marshal( GlobalSession session, WritableChannel channel ) throws IOException
        {
            channel.putLong( session.globalId().sessionId().getMostSignificantBits() );
            channel.putLong( session.globalId().sessionId().getLeastSignificantBits() );
            MemberId.Marshal.INSTANCE.marshal( session.globalId().owner(), channel );

            channel.putLong( session.operationId().localSessionId() );
            channel.putLong( session.operationId().sequenceNumber() );
        }

        @Override
        public GlobalSession unmarshal0( ReadableChannel channel ) throws IOException, EndOfStreamException
        {
            long mostSigBits = channel.getLong();
            long leastSigBits = channel.getLong();
            MemberId owner = MemberId.Marshal.INSTANCE.unmarshal( channel );

            long localSessionId = channel.getLong();
            long sequenceNumber = channel.getLong();

            return new GlobalSession(
                    new GlobalSessionId( new UUID( mostSigBits, leastSigBits ), owner ),
                    new LocalOperationId( localSessionId, sequenceNumber ) );
        }
    }
}
