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
package org.neo4j.coreedge.helper;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.ScheduledThreadPoolExecutor;

import org.neo4j.helpers.NamedThreadFactory;

import static java.lang.String.format;
import static java.util.concurrent.TimeUnit.MILLISECONDS;

/**
 * Useful for debugging using collected statistics, e.g. for timing.
 *
 * A typical usage would be to instantiate a StatContext in the context where
 * we want to capture statistics, e.g.:
 *
 *   StatContext lockTimings = StatUtil.create( "locks-" + id, 30_000, true );
 *
 * which will print statistics captured under lockTimings every 30 seconds and
 * clear the gathered statistics at that point as well due to specifying
 * clearAfterPrint=true. If no data points were gathered during the 30 seconds
 * then nothing is printed.
 *
 * Note that the name selected for the statistic should generally be unique since the
 * printing service will replace an old one under the same name, but sometimes this is
 * the desired behaviour, for example when tracking for a particular server id. This
 * is exemplified above by ("locks-" + id).
 *
 * The statistics are captured by utilizing the StatContext as such:
 *
 *   TimingContext timing = lockTimings.time();
 *     ... thing to measure ...
 *   timing.end();
 *
 * The TimingContext simplifies gathering statistics for time (System.currentTimeMillis()).
 *
 * It is also possible to gather arbitrary data using StatContext#collect(double).
 */
public class StatUtil
{
    private static class StatPrinterService extends ScheduledThreadPoolExecutor
    {
        private static final StatPrinterService INSTANCE = new StatPrinterService();

        private StatPrinterService()
        {
            super( 1, new NamedThreadFactory( "stat-printer" , Thread.NORM_PRIORITY, true ) );
            super.setRemoveOnCancelPolicy( true );
        }
    }

    public static class StatContext
    {
        private static final int N_BUCKETS = 10; // values >= Math.pow( 10, N_BUCKETS-1 ) all go into the last bucket

        private String name;
        private BasicStats bucket[] = new BasicStats[N_BUCKETS];
        private long totalCount;

        private StatContext( String name )
        {
            this.name = name;
            clear();
        }

        public synchronized void clear()
        {
            for ( int i = 0; i < N_BUCKETS; i++ )
            {
                bucket[i] = new BasicStats();
            }
            totalCount = 0;
        }

        public void collect( double value )
        {
            int bucketIndex = bucketFor( value );

            synchronized ( this )
            {
                totalCount++;
                bucket[bucketIndex].collect( value );
            }
        }

        private int bucketFor( double value )
        {
            int bucketIndex;
            if ( value <= 0 )
            {
                // we do not have buckets for negative values, we assume user doesn't measure such things
                // however, if they do, it will all be collected in bucket 0
                bucketIndex = 0;
            }
            else
            {
                bucketIndex = (int) Math.log10( value );
                bucketIndex = Math.min( bucketIndex, N_BUCKETS - 1 );
            }
            return bucketIndex;
        }

        public TimingContext time()
        {
            return new TimingContext( this );
        }

        public long totalCount()
        {
            return totalCount;
        }

        public synchronized String getText( boolean clear )
        {
            String text = toString();

            if( clear )
            {
                clear();
            }

            return text;
        }

        @Override
        public synchronized String toString()
        {
            StringBuilder output = new StringBuilder();
            output.append( format( "%s%n", name ) );
            for ( BasicStats stats : bucket )
            {
                if( stats.count > 0 )
                {
                    output.append( format( "%s%n", stats ) );
                }
            }
            return output.toString();
        }
    }

    public static StatContext create( String name )
    {
        return new StatContext( name );
    }

    private static Map<String,ScheduledFuture> printingJobs = new HashMap<>();

    public static synchronized StatContext create( String name, long printEveryMs )
    {
        return create( name, printEveryMs, false );
    }

    public static synchronized StatContext create( String name, long printEveryMs, boolean clearAfterPrint )
    {
        StatContext statContext = new StatContext( name );

        ScheduledFuture job = printingJobs.remove( name );

        if( job != null )
        {
            job.cancel( true );
            System.out.println( "WARNING: Replacing printer for: " + name );
        }

        job = StatPrinterService.INSTANCE.scheduleAtFixedRate( () -> {
            if ( statContext.totalCount() > 0 )
            {
                String text = statContext.getText( clearAfterPrint );
                System.out.print( text );
            }
        }, printEveryMs, printEveryMs, MILLISECONDS );

        printingJobs.put( name, job );

        return statContext;
    }

    public static class TimingContext
    {
        private final StatContext context;
        private final long startTime = System.currentTimeMillis();

        TimingContext( StatContext context )
        {
            this.context = context;
        }

        public void end()
        {
            context.collect( System.currentTimeMillis() - startTime );
        }
    }

    private static class BasicStats
    {
        private Double min;
        private Double max;

        private Double avg = 0.;
        private long count;

        void collect( double val )
        {
            count++;
            avg = avg + (val - avg) / count;

            min = min == null ? val : Math.min( min, val );
            max = max == null ? val : Math.max( max, val );
        }

        @Override
        public String toString()
        {
            return format( "BasicStats{min=%s, max=%s, avg=%s, count=%d}", min, max, avg, count );
        }
    }
}
