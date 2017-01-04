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
package org.neo4j.causalclustering.core.state.machines.locks.client;

import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;

import org.neo4j.causalclustering.core.state.machines.locks.LockMode;
import org.neo4j.collection.primitive.PrimitiveLongCollections;
import org.neo4j.storageengine.api.lock.ResourceType;

// TODO: Handle shared vs exclusive semantics? Utilize only one combined map and state per entity?

/**
 * Functions as a cache for remotely acquired locks, so that multiple remote calls
 * for the same entity can be avoided. It allows for filtering out locks which are
 * already remotely acquired as well as keeping a local reference counter.
 *
 * The functions in this class use try-semantics when acquiring locks because it is
 * generally expected that a remotely acquired lock is immediately available locally.
 * Failure to acquire a lock immediately will throw a DeadlockDetectedException.
 */
class LocksCache
{
    private final Map<ResourceType,Map<Long/*id*/,ReferenceCounter>> sharedLocks = new HashMap<>();
    private final Map<ResourceType,Map<Long/*id*/,ReferenceCounter>> exclusiveLocks = new HashMap<>();

    /**
     * Filters out shared locks which are already locally held and thus need not be
     * remotely acquired yet again. The local reference counters will be increased
     * for the filtered locks and a the remaining locks will be returned.
     *
     * N.B: this method takes ownership of resourceIds and may modify it
     *
     * @param type The type of the resource.
     * @param ids  The ids of the resources.
     */
    long[] filter( LockMode mode, ResourceType type, long[] ids )
    {
        Map<Long,ReferenceCounter> lockMap = getLockMap( mode, type );

        int newIdCount = 0;
        for ( int i = 0; i < ids.length; i++ )
        {
            ReferenceCounter refCount = lockMap.get( ids[i] );
            if ( refCount != null )
            {
                refCount.increment();
            }
            else
            {
                /* We reuse the array for keeping the unfiltered locks. */
                ids[newIdCount++] = ids[i];
            }
        }

        if ( newIdCount == 0 )
        {
            return PrimitiveLongCollections.EMPTY_LONG_ARRAY;
        }
        return newIdCount == ids.length ? ids : Arrays.copyOf( ids, newIdCount );
    }

    /**
     * Registers the lock locally in the cache. It is illegal to call this method on
     * locks which are already taken, meaning that the filtering function must first
     * have been called and only the new ids should be passed in here.
     *
     * @param type The type of the resource.
     * @param id   The id of the resource.
     */
    void register( LockMode mode, ResourceType type, long id )
    {
        Map<Long,ReferenceCounter> lockMap = getLockMap( mode, type );

        ReferenceCounter previous = lockMap.putIfAbsent( id, new ReferenceCounter( 1 ) );
        if ( previous != null )
        {
            throw new IllegalStateException();
        }
    }

    private Map<Long,ReferenceCounter> getLockMap( LockMode mode, ResourceType type )
    {
        switch ( mode )
        {
        case SHARED:
            return sharedLocks.computeIfAbsent( type, ignored -> new HashMap<>() );
        case EXCLUSIVE:
            return exclusiveLocks.computeIfAbsent( type, ignored -> new HashMap<>() );
        default:
            throw new IllegalStateException();
        }
    }
}
