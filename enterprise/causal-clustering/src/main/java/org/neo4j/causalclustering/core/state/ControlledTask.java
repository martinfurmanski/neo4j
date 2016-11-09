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

import org.neo4j.function.ThrowingAction;

/**
 * Represents a controlled task that is repeatedly invoked until it is
 * either stopped or paused. A paused task can be resumed.
 */
class ControlledTask
{
    private final Thread thread;

    private boolean pauseRequested;
    private boolean currentlyPaused;

    private boolean shutdown;

    ControlledTask( Runnable task, String name )
    {
        this.thread = new Thread( () -> controller( task ), name );
    }

    private void controller( Runnable task )
    {
        while ( true )
        {
            boolean hasBeenShutdown = waitForActive();

            if ( hasBeenShutdown )
            {
                break;
            }

            task.run();
        }
    }

    private synchronized boolean waitForActive()
    {
        while ( pauseRequested && !shutdown )
        {
            currentlyPaused = true;
            notifyAll();

            ignoringInterrupts( this::wait );
        }

        currentlyPaused = false;
        return shutdown;
    }

    /**
     * Pauses the running task. Does not return until the task is idle.
     */
    synchronized void pause()
    {
        pauseRequested = true;

        while ( pauseRequested && !currentlyPaused )
        {
            ignoringInterrupts( this::wait );
        }
    }

    /**
     * Resumes the previously paused task.
     */
    synchronized void resume()
    {
        pauseRequested = false;
        notifyAll();
    }

    private synchronized void setShutdown( boolean state )
    {
        shutdown = state;
        notifyAll();
    }

    /**
     * Starts the task.
     */
    void start()
    {
        thread.start();
    }

    /**
     * Stops the task. Does not return until the task is finished.
     */
    void stop()
    {
        setShutdown( true );
        ignoringInterrupts( thread::join );
    }

    private void ignoringInterrupts( ThrowingAction<InterruptedException> action )
    {
        try
        {
            action.apply();
        }
        catch ( InterruptedException e )
        {
            // ignored
        }
    }
}
