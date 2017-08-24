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
package org.neo4j.causalclustering.core.replication;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Stream;

import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Transaction;
import org.neo4j.kernel.api.exceptions.InvalidArgumentsException;
import org.neo4j.kernel.internal.GraphDatabaseAPI;
import org.neo4j.logging.Log;
import org.neo4j.procedure.Context;
import org.neo4j.procedure.Description;
import org.neo4j.procedure.Name;
import org.neo4j.procedure.Procedure;

import static java.lang.Math.toIntExact;
import static org.neo4j.procedure.Mode.DBMS;

@SuppressWarnings( "unused" )
public class TransactionBenchmarkProcedure
{
    private static long startTime;
    private static List<Worker> workers;
    private static List<Writer> writers;

    @Context
    public Log log;

    @Context
    public GraphDatabaseAPI db;

    @Description( "Start the benchmark." )
    @Procedure( name = "dbms.transaction.benchmark.start", mode = DBMS )
    public synchronized void start( @Name( "wThreads" ) Long wThreads, @Name( "rThreads" ) Long rThreads, @Name( "nodes" ) Long nodes )
            throws InvalidArgumentsException, IOException
    {
        if ( writers != null )
        {
            throw new IllegalStateException( "Already running." );
        }

        log.info( "Starting transaction benchmark procedure" );

        startTime = System.currentTimeMillis();
        writers = new ArrayList<>( toIntExact( wThreads ) );
        workers = new ArrayList<>( toIntExact( wThreads + rThreads ) );

        for ( int i = 0; i < rThreads; i++ )
        {
            Worker worker = new Worker( new Reader() );
            workers.add( worker );
            worker.start();
        }

        for ( int i = 0; i < wThreads; i++ )
        {
            Writer writer = new Writer( toIntExact( nodes ) );
            writers.add( writer );

            Worker worker = new Worker( writer );
            workers.add( worker );

            worker.start();
        }
    }

    @Description( "Stop a running benchmark." )
    @Procedure( name = "dbms.transaction.benchmark.stop", mode = DBMS )
    public synchronized Stream<TransactionBenchmarkResult> stop() throws InvalidArgumentsException, IOException, InterruptedException
    {
        if ( writers == null )
        {
            throw new IllegalStateException( "Not running." );
        }

        log.info( "Stopping transaction benchmark procedure" );

        for ( Worker worker : workers )
        {
            worker.stop();
        }

        for ( Worker worker : workers )
        {
            worker.join();
        }

        long runTime = System.currentTimeMillis() - startTime;

        long totalRequests = 0;
        long totalNodes = 0;

        for ( Writer writer : writers )
        {
            totalRequests += writer.totalRequests;
            totalNodes += writer.totalNodes;
        }

        writers = null;

        return Stream.of( new TransactionBenchmarkResult( totalRequests, totalNodes, runTime ) );
    }

    private class Worker implements Runnable
    {
        private final Runnable task;

        private Thread t;
        private volatile boolean stopped;

        Worker( Runnable task )
        {
            this.task = task;
        }

        void start()
        {
            t = new Thread( this );
            t.start();
        }

        @Override
        public void run()
        {
            try
            {
                while ( !stopped )
                {
                    task.run();
                }
            }
            catch ( Throwable e )
            {
                log.error( "Worker exception", e );
            }
        }

        void stop() throws InterruptedException
        {
            stopped = true;
        }

        void join() throws InterruptedException
        {
            t.join();
        }
    }

    class Writer implements Runnable
    {
        private final int nodes;

        long totalRequests;
        long totalNodes;

        Writer( int nodes )
        {
            this.nodes = nodes;
        }

        @Override
        public void run()
        {
            try ( Transaction tx = db.beginTx() )
            {
                for ( int i = 0; i < nodes; i++ )
                {
                    db.createNode();
                }
                tx.success();
            }
            catch ( Exception e )
            {
                log.warn( "Exception", e );
            }

            totalRequests++;
            totalNodes += nodes;
        }
    }

    class Reader implements Runnable
    {
        @Override
        public void run()
        {
            try ( Transaction tx = db.beginTx() )
            {
                int i = 0;
                for ( Node node : db.getAllNodes() )
                {
                    node.getLabels();
                    if ( i++ > 10000 )
                    {
                        break;
                    }
                }
                tx.success();
            }
            catch ( Exception e )
            {
                log.warn( "Exception", e );
            }
        }
    }
}
