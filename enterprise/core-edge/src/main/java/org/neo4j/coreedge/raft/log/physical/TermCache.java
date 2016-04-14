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
package org.neo4j.coreedge.raft.log.physical;

import java.io.IOException;
import java.util.Map;
import java.util.NavigableMap;
import java.util.TreeMap;

import org.neo4j.coreedge.raft.log.RaftLogCompactedException;
import org.neo4j.cursor.IOCursor;

/**
 * The term changes infrequently, so it makes sense to keep an efficient cache of all terms.
 *
 * The implementation is based around a TreeSet and it's floor operation. Essentially there
 * is one tree node for each term change, mapping from log index -> term. Indexes in between
 * term changes are looked up using the floor operation.
 */
class TermCache
{
    static final long UNKNOWN_TERM = Long.MAX_VALUE;

    private final NavigableMap<Long/*floorIndex*/,Long/*term*/> cache;

    private long currentFloorIndex;
    private long currentTerm = -1;
    private final long minIndex = -1;
    private final long maxIndex = -1;

    TermCache()
    {
        this.cache = new TreeMap<>();
    }

    void populateWith( EntryStore entryStore, long fromIndex ) throws IOException, RaftLogCompactedException
    {
        IOCursor<EntryRecord> cursor = entryStore.getEntriesFrom( fromIndex );
        while ( cursor.next() )
        {
            EntryRecord entry = cursor.get();
            long term = entry.logEntry().term();
            if ( term != currentTerm )
            {
                populate( entry.logIndex(), term );
                currentTerm = term;
            }
        }
    }

    /**
     * Must be called on each term switching index to remain valid.
     *
     * @param logIndex The index at which the term switched.
     * @param term     The term to which we switched.
     */
    void populate( long logIndex, long term )
    {
        // TODO: Populate the cache!
    }

    /**
     * @param logIndex The index for which to lookup the term.
     * @return Returns the term of the index or -1 if it is unknown.
     */
    long lookup( long logIndex )
    {
        if ( logIndex >= currentFloorIndex )
        {
            return currentTerm;
        }

        Map.Entry<Long,Long> entry = cache.floorEntry( logIndex );
        return entry != null ? entry.getValue() : UNKNOWN_TERM;
    }
}
