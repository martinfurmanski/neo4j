package org.neo4j.causalclustering.core.state;

import org.junit.Test;

import java.util.concurrent.atomic.AtomicInteger;

import static java.util.concurrent.TimeUnit.SECONDS;
import static org.hamcrest.Matchers.greaterThan;
import static org.junit.Assert.assertEquals;
import static org.neo4j.test.assertion.Assert.assertEventually;

public class ControlledTaskTest
{
    private AtomicInteger value = new AtomicInteger();
    private ControlledTask task = new ControlledTask( value::incrementAndGet, "increment" );

    @Test
    public void shouldRunTaskContinuouslyUntilStopped() throws Exception
    {
        // when
        task.start();

        // then
        assertEventually( "value should increase", value::get, greaterThan( 1000 ), 5, SECONDS );

        // when
        task.stop();

        int lastA = value.get();
        Thread.sleep( 10 );
        int lastB = value.get();

        // then
        assertEquals( lastA, lastB );
    }

    @Test
    public void shouldNotRunTaskWhilePaused() throws Exception
    {
        // when
        task.start();

        // then
        assertEventually( "value should increase", value::get, greaterThan( 1000 ), 5, SECONDS );

        // when
        task.pause();

        int lastA = value.get();
        Thread.sleep( 10 );
        int lastB = value.get();

        // then
        assertEquals( lastA, lastB );

        // when
        task.resume();

        // then
        assertEventually( "value should increase", value::get, greaterThan( lastB + 1000 ), 5, SECONDS );
        task.stop();
    }
}
