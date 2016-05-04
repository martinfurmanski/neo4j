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

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.SortedMap;

import org.neo4j.coreedge.raft.log.DamagedLogStorageException;
import org.neo4j.coreedge.raft.replication.ReplicatedContent;
import org.neo4j.coreedge.raft.state.ChannelMarshal;
import org.neo4j.coreedge.raft.state.UnexpectedEndOfStreamException;
import org.neo4j.cursor.IOCursor;
import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.logging.Log;
import org.neo4j.logging.LogProvider;

/**
 * Recovers all the state required for operating the RAFT log and does some simple
 * verifications; e.g. checking for gaps, verifying headers.
 */
// TODO: Verify headers, check for gaps in version numbers, ...
class RecoveryProtocol
{
    private final FileSystemAbstraction fileSystem;
    private final FileNames fileNames;
    private final ChannelMarshal<ReplicatedContent> contentMarshal;
    private final LogProvider logProvider;
    private final Log log;

    RecoveryProtocol( FileSystemAbstraction fileSystem, FileNames fileNames,
            ChannelMarshal<ReplicatedContent> contentMarshal, LogProvider logProvider )
    {
        this.fileSystem = fileSystem;
        this.fileNames = fileNames;
        this.contentMarshal = contentMarshal;
        this.logProvider = logProvider;
        this.log = logProvider.getLog( getClass() );
    }

    State run() throws IOException, DamagedLogStorageException
    {
        State state = new State();
        SortedMap<Long,File> files = fileNames.getAllFiles( fileSystem, log );

        if ( files.entrySet().isEmpty() )
        {
            state.segments = new Segments( fileSystem, fileNames, Collections.emptyList(), contentMarshal, logProvider, -1 );
            state.segments.createNext( -1, -1, -1 );
            return state;
        }

        List<SegmentFile> segmentFiles = new ArrayList<>();
        long firstVersion = files.firstKey();

        for ( Map.Entry<Long,File> entry : files.entrySet() )
        {
            try
            {
                Long version = entry.getKey();
                File file = entry.getValue();

                SegmentFile segment;
                try
                {
                    segment = SegmentFile.load( fileSystem, file, contentMarshal, logProvider );
                }
                catch ( UnexpectedEndOfStreamException e )
                {
                    // TODO Handle
                    throw new RuntimeException( e );
                }

                segmentFiles.add( segment );

                if ( version == firstVersion )
                {
                    state.prevIndex = segment.header().prevIndex();
                    state.prevTerm = segment.header().prevTerm();
                }

                // checkVersionStrictlyMonotonic( fileNameVersion, state );
                // checkVersionMatches( fileNameVersion, header.version() );
                // check term
            }
            catch ( IOException e )
            {
                log.error( "Error during recovery", e );
            }
        }

        SegmentFile last = segmentFiles.get( segmentFiles.size() - 1 );

        state.segments = new Segments( fileSystem, fileNames, segmentFiles, contentMarshal, logProvider, firstVersion - 1 );
        state.appendIndex = last.header().prevIndex();
        state.currentTerm = last.header().prevTerm();

        long firstIndexInLastSegmentFile = last.header().prevIndex() + 1;
        try ( IOCursor<EntryRecord> reader =  last.getReader( firstIndexInLastSegmentFile ) )
        {
            while ( reader.next() )
            {
                EntryRecord entry = reader.get();
                state.appendIndex = entry.logIndex();
                state.currentTerm = entry.logEntry().term();
            }
        }
        catch ( DisposedException e )
        {
            throw new RuntimeException( "Unexpected exception", e );
        }

        return state;
    }

    private void checkVersionStrictlyMonotonic( long fileNameVersion, State state )
    {
        // TODO
    }

    private void checkVersionMatches( long expectedVersion, long headerVersion )
    {
        // TODO
    }
}
