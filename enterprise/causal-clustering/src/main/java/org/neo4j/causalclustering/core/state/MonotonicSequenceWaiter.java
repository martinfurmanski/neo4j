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
package org.neo4j.causalclustering.core.state;

/**
 * Implements a wait/notify scheme around a monotonic sequence, where a writer
 * increments the sequence while a reader can wait for updates.
 */
class MonotonicSequenceWaiter
{
    private long value;
    private boolean disabled;

    MonotonicSequenceWaiter( long initialValue )
    {
        this.value = initialValue;
    }

    /**
     * Will wait for and return value above limit, unless the waiter is disabled.
     *
     * @param limit The lower bound (exclusive) of the value to wait for.
     *
     * @return A value above the limit (unless disabled). When disabled
     *         the current value is returned.
     *
     * @throws InterruptedException If the waiting was interrupted.
     */
    synchronized long awaitGreaterThan( long limit ) throws InterruptedException
    {
        while ( value <= limit && !disabled )
        {
            wait();
        }

        return value;
    }

    synchronized void set( long newValue )
    {
        if ( newValue > value )
        {
            value = newValue;
            notifyAll();
        }
    }

    public synchronized long get()
    {
        return value;
    }

    synchronized void disable()
    {
        disabled = true;
        notifyAll();
    }

    synchronized void enable()
    {
        disabled = false;
        notifyAll();
    }
}
