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
import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import org.neo4j.coreedge.raft.log.physical.OpenEndRangeMap.ValueRange;
import org.neo4j.coreedge.raft.replication.ReplicatedContent;
import org.neo4j.coreedge.raft.state.ChannelMarshal;
import org.neo4j.helpers.collection.Iterators;
import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.logging.Log;
import org.neo4j.logging.LogProvider;

class Segments
{
    private final OpenEndRangeMap<Long/*minIndex*/,SegmentFile> rangeMap = new OpenEndRangeMap<>();
    private final List<SegmentFile> segmentFiles;
    private final Log log;

    private FileSystemAbstraction fileSystem;
    private final FileNames fileNames;
    private final ChannelMarshal<ReplicatedContent> contentMarshal;
    private final LogProvider logProvider;
    private long currentVersion;

    Segments( FileSystemAbstraction fileSystem, FileNames fileNames, List<SegmentFile> segmentFiles,
            ChannelMarshal<ReplicatedContent> contentMarshal, LogProvider logProvider, long currentVersion )
    {
        this.fileSystem = fileSystem;
        this.fileNames = fileNames;
        this.segmentFiles = new ArrayList<>( segmentFiles );
        this.contentMarshal = contentMarshal;
        this.logProvider = logProvider;
        this.log = logProvider.getLog( getClass() );
        this.currentVersion = currentVersion;

        populateRangeMap();
    }

    private void populateRangeMap()
    {
        for ( SegmentFile segment : segmentFiles )
        {
            rangeMap.replaceFrom( segment.header().prevIndex() + 1, segment );
        }
    }

    synchronized SegmentFile createNext( long prevFileLastIndex, long prevIndex, long prevTerm ) throws IOException
    {
        currentVersion++;
        SegmentHeader header = new SegmentHeader( prevFileLastIndex, currentVersion, prevIndex, prevTerm );

        File file = fileNames.getForVersion( currentVersion );
        SegmentFile segment = SegmentFile.create( fileSystem, file, contentMarshal, logProvider, header );
        // TODO: Force base directory... probably not possible using fsa.
        segment.flush();

        segmentFiles.add( segment );

        Collection<SegmentFile> removed = rangeMap.replaceFrom( prevIndex + 1, segment );

        removed.forEach( removedSegment -> {
            try
            {
                removedSegment.markForDisposal( this::disposeHandler );
            }
            catch ( DisposedException e )
            {
                log.warn( "The segment was already marked for disposal.", e );
            }
        } );

        return segment;
    }

    ValueRange<Long,SegmentFile> getForIndex( long logIndex )
    {
        return rangeMap.lookup( logIndex );
    }

    SegmentFile last()
    {
        return rangeMap.last();
    }

    public synchronized SegmentFile prune( long safeIndex )
    {
        Collection<SegmentFile> forDisposal = new ArrayList<>();
        SegmentFile oldestNotDisposed = collectSegmentsForDisposal( safeIndex, forDisposal );

        for ( SegmentFile segment : forDisposal )
        {
            try
            {
                segment.markForDisposal( this::disposeHandler );
            }
            catch ( DisposedException e )
            {
                log.warn( "Already marked for disposal", e );
            }
        }
        return oldestNotDisposed;
    }

    private SegmentFile collectSegmentsForDisposal( long safeIndex, Collection<SegmentFile> forDisposal )
    {
        Iterator<SegmentFile> itr = segmentFiles.iterator();
        SegmentFile prev = itr.next(); // there is always at least one segment
        SegmentFile oldestNotDisposed = prev;

        while ( itr.hasNext() )
        {
            SegmentFile segment = itr.next();
            if ( segment.header().prevFileLastIndex() <= safeIndex )
            {
                forDisposal.add( prev );
                oldestNotDisposed = segment;
            }
            else
            {
                break;
            }
            prev = segment;
        }

        return oldestNotDisposed;
    }

    private synchronized void disposeHandler()
    {
        Iterator<SegmentFile> filesItr = segmentFiles.iterator();
        SegmentFile segment;
        while ( filesItr.hasNext() && (segment = filesItr.next()).isDisposed() )
        {
            segment.delete();
            Iterator<Map.Entry<Long,SegmentFile>> rangeItr = rangeMap.entrySet().iterator();
            Map.Entry<Long,SegmentFile> firstRange = Iterators.firstOrNull( rangeItr );
            if ( firstRange != null && firstRange.getValue() == segment )
            {
                rangeItr.remove();
            }
            filesItr.remove();
        }
    }
}
