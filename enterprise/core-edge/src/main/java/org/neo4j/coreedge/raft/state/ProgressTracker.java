package org.neo4j.coreedge.raft.state;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import org.neo4j.coreedge.raft.replication.DistributedOperation;
import org.neo4j.coreedge.raft.replication.Progress;
import org.neo4j.coreedge.raft.replication.session.GlobalSession;
import org.neo4j.coreedge.raft.replication.session.LocalOperationId;

/**
 * Keeps track of operations in progress. Operations move through two phases:
 *  - waiting for replication
 *  - waiting for result
 */
public class ProgressTracker
{
    private final Map<LocalOperationId,Progress> tracker = new ConcurrentHashMap<>();
    private final GlobalSession myGlobalSession;

    public ProgressTracker( GlobalSession myGlobalSession )
    {
        this.myGlobalSession = myGlobalSession;
    }

    public Progress start( DistributedOperation operation )
    {
        assert operation.globalSession().equals( myGlobalSession );

        Progress progress = new Progress();
        tracker.put( operation.operationId(), progress );
        return progress;
    }

    public void trackReplication( DistributedOperation operation )
    {
        if( !operation.globalSession().equals( myGlobalSession ) )
        {
            return;
        }

        Progress progress = tracker.get( operation.operationId() );
        if( progress != null )
        {
            progress.setReplicated();
        }
    }

    public void trackResult( DistributedOperation operation, Result result )
    {
        if( !operation.globalSession().equals( myGlobalSession ) )
        {
            return;
        }

        Progress progress = tracker.remove( operation.operationId() );

        if ( progress != null )
        {
            result.apply( progress.futureResult() );
        }
    }

    public void end( DistributedOperation operation )
    {
        tracker.remove( operation.operationId() );
    }

    public void retriggerReplication()
    {
        tracker.forEach( ( ignored, progress ) -> progress.triggerReplication() );
    }
}
