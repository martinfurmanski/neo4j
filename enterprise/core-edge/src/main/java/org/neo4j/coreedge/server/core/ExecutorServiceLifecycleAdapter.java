package org.neo4j.coreedge.server.core;

import java.util.concurrent.ExecutorService;

import org.neo4j.kernel.lifecycle.LifecycleAdapter;

public class ExecutorServiceLifecycleAdapter extends LifecycleAdapter
{
    private final ExecutorService executorService;

    public ExecutorServiceLifecycleAdapter( ExecutorService executorService )
    {
        this.executorService = executorService;
    }

    @Override
    public void shutdown() throws Throwable
    {
        executorService.shutdown();
    }
}
