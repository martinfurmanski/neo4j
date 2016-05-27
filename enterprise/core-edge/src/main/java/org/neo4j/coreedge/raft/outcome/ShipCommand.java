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
package org.neo4j.coreedge.raft.outcome;

import org.neo4j.coreedge.raft.LeaderContext;
import org.neo4j.coreedge.raft.log.RaftLogEntry;
import org.neo4j.coreedge.raft.replication.shipping.RaftLogShipper;

import static java.lang.String.format;

import java.io.IOException;
import java.util.Arrays;
import java.util.Objects;

public abstract class ShipCommand
{
    public abstract <MEMBER> void applyTo( RaftLogShipper<MEMBER> raftLogShipper, LeaderContext leaderContext )
            throws IOException;

    public static class Mismatch extends ShipCommand
    {
        private final long lastRemoteAppendIndex;
        private final Object target;

        public Mismatch( long lastRemoteAppendIndex, Object target )
        {
            this.lastRemoteAppendIndex = lastRemoteAppendIndex;
            this.target = target;
        }

        @Override
        public void applyTo( RaftLogShipper raftLogShipper, LeaderContext leaderContext ) throws IOException
        {
            if ( raftLogShipper.identity().equals( target ) )
            {
                raftLogShipper.onMismatch( lastRemoteAppendIndex, leaderContext );
            }
        }

        @Override
        public boolean equals( Object o )
        {
            if ( this == o )
            {
                return true;
            }
            if ( o == null || getClass() != o.getClass() )
            {
                return false;
            }

            Mismatch mismatch = (Mismatch) o;

            if ( lastRemoteAppendIndex != mismatch.lastRemoteAppendIndex )
            {
                return false;
            }
            return target.equals( mismatch.target );

        }

        @Override
        public int hashCode()
        {
            int result = (int) (lastRemoteAppendIndex ^ (lastRemoteAppendIndex >>> 32));
            result = 31 * result + target.hashCode();
            return result;
        }

        @Override
        public String toString()
        {
            return format( "Mismatch{lastRemoteAppendIndex=%d, target=%s}", lastRemoteAppendIndex, target );
        }
    }

    public static class Match extends ShipCommand
    {
        private final long newMatchIndex;
        private final Object target;

        public Match( long newMatchIndex, Object target )
        {
            this.newMatchIndex = newMatchIndex;
            this.target = target;
        }

        public <MEMBER> void applyTo( RaftLogShipper<MEMBER> raftLogShipper, LeaderContext leaderContext ) throws
                IOException
        {
            if ( raftLogShipper.identity().equals( target ) )
            {
                raftLogShipper.onMatch( newMatchIndex, leaderContext );
            }
        }

        @Override
        public boolean equals( Object o )
        {
            if ( this == o )
            {
                return true;
            }
            if ( o == null || getClass() != o.getClass() )
            {
                return false;
            }

            Match match = (Match) o;

            if ( newMatchIndex != match.newMatchIndex )
            {
                return false;
            }
            return target.equals( match.target );

        }

        @Override
        public int hashCode()
        {
            int result = (int) (newMatchIndex ^ (newMatchIndex >>> 32));
            result = 31 * result + target.hashCode();
            return result;
        }

        @Override
        public String toString()
        {
            return format( "Match{newMatchIndex=%d, target=%s}", newMatchIndex, target );
        }
    }

    public static class NewEntry extends ShipCommand
    {
        private final long prevLogIndex;
        private final long prevLogTerm;
        private final RaftLogEntry[] newLogEntries;

        public NewEntry( long prevLogIndex, long prevLogTerm, RaftLogEntry[] newLogEntries )
        {
            this.prevLogIndex = prevLogIndex;
            this.prevLogTerm = prevLogTerm;
            this.newLogEntries = newLogEntries;
        }

        @Override
        public <MEMBER> void applyTo( RaftLogShipper<MEMBER> raftLogShipper, LeaderContext leaderContext ) throws IOException
        {
            raftLogShipper.onNewEntry( prevLogIndex, prevLogTerm, newLogEntries, leaderContext );
        }

        @Override
        public boolean equals( Object o )
        {
            if ( this == o )
            { return true; }
            if ( o == null || getClass() != o.getClass() )
            { return false; }
            NewEntry newEntry = (NewEntry) o;
            return prevLogIndex == newEntry.prevLogIndex &&
                   prevLogTerm == newEntry.prevLogTerm &&
                   Arrays.equals( newLogEntries, newEntry.newLogEntries );
        }

        @Override
        public int hashCode()
        {
            return Objects.hash( prevLogIndex, prevLogTerm, newLogEntries );
        }

        @Override
        public String toString()
        {
            return "NewEntry{" +
                   "prevLogIndex=" + prevLogIndex +
                   ", prevLogTerm=" + prevLogTerm +
                   ", newLogEntries=" + Arrays.toString( newLogEntries ) +
                   '}';
        }
    }

    public static class CommitUpdate extends ShipCommand
    {
        @Override
        public <MEMBER> void applyTo( RaftLogShipper<MEMBER> raftLogShipper, LeaderContext leaderContext ) throws IOException
        {
            raftLogShipper.onCommitUpdate( leaderContext );
        }

        @Override
        public String toString()
        {
            return "CommitUpdate{}";
        }
    }
}
