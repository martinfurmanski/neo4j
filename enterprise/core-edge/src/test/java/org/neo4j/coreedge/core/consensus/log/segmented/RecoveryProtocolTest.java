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
package org.neo4j.coreedge.core.consensus.log.segmented;

import org.junit.Before;
import org.junit.Test;

import java.io.File;
import java.io.IOException;
import java.util.concurrent.ThreadLocalRandom;

import org.neo4j.coreedge.core.consensus.ReplicatedInteger;
import org.neo4j.coreedge.core.consensus.log.DummyRaftableContentSerializer;
import org.neo4j.coreedge.core.consensus.log.RaftLogEntry;
import org.neo4j.coreedge.core.replication.ReplicatedContent;
import org.neo4j.coreedge.messaging.marsalling.ChannelMarshal;
import org.neo4j.graphdb.mockfs.EphemeralFileSystemAbstraction;
import org.neo4j.io.fs.StoreChannel;
import org.neo4j.kernel.impl.transaction.log.PhysicalFlushableChannel;
import org.neo4j.logging.NullLogProvider;
import org.neo4j.time.FakeClock;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.fail;

public class RecoveryProtocolTest
{
    private EphemeralFileSystemAbstraction fsa = new EphemeralFileSystemAbstraction();
    private ChannelMarshal<ReplicatedContent> contentMarshal = new DummyRaftableContentSerializer();
    private final File root = new File( "root" );
    private FileNames fileNames = new FileNames( root );
    private SegmentHeader.Marshal headerMarshal = new SegmentHeader.Marshal();
    private ReaderPool readerPool = new ReaderPool( 0, NullLogProvider.getInstance(), fileNames, fsa, new FakeClock() );

    @Before
    public void setup()
    {
        fsa.mkdirs( root );
    }

    @Test
    public void shouldReturnEmptyStateOnEmptyDirectory() throws Exception
    {
        // given
        RecoveryProtocol protocol = new RecoveryProtocol( fsa, fileNames, readerPool, contentMarshal, NullLogProvider.getInstance(), 1 );

        // when
        State state = protocol.run();

        // then
        assertEquals( -1, state.appendIndex );
        assertEquals( -1, state.terms.latest() );
        assertEquals( -1, state.prevIndex );
        assertEquals( -1, state.prevTerm );
        assertEquals( 0, state.segments.last().header().version() );
    }

    @Test
    public void shouldFailIfThereAreGapsInVersionNumberSequence() throws Exception
    {
        // given
        createLogFile( fsa, -1, 0, 0, -1, -1 );
        createLogFile( fsa, 5, 2, 2, 5, 0 );

        RecoveryProtocol protocol = new RecoveryProtocol( fsa, fileNames, readerPool, contentMarshal, NullLogProvider.getInstance(), 1 );

        try
        {
            // when
            protocol.run();
            fail( "Expected an exception" );
        }
        catch ( DamagedLogStorageException e )
        {
            // expected
        }
    }

    @Test
    public void shouldFailIfTheVersionNumberInTheHeaderAndFileNameDiffer() throws Exception
    {
        // given
        createLogFile( fsa, -1, 0, 1, -1, -1 );

        RecoveryProtocol protocol = new RecoveryProtocol( fsa, fileNames, readerPool, contentMarshal, NullLogProvider.getInstance(), 1 );

        try
        {
            // when
            protocol.run();
            fail( "Expected an exception" );
        }
        catch ( DamagedLogStorageException e )
        {
            // expected
        }
    }

    @Test
    public void shouldFailIfANonLastFileIsMissingHeader() throws Exception
    {
        // given
        createLogFile( fsa, -1, 0, 0, -1, -1 );
        createEmptyLogFile( fsa, 1 );
        createLogFile( fsa, -1, 2, 2, -1, -1 );

        RecoveryProtocol protocol = new RecoveryProtocol( fsa, fileNames, readerPool, contentMarshal, NullLogProvider.getInstance(), 1 );

        try
        {
            // when
            protocol.run();
            fail( "Expected an exception" );
        }
        catch ( DamagedLogStorageException e )
        {
            // expected
        }
    }

    @Test
    public void shouldRecoverEvenIfLastHeaderIsMissing() throws Exception
    {
        // given
        createLogFile( fsa, -1, 0, 0, -1, -1 );
        createEmptyLogFile( fsa, 1 );

        RecoveryProtocol protocol = new RecoveryProtocol( fsa, fileNames, readerPool, contentMarshal, NullLogProvider.getInstance(), 1 );

        // when
        protocol.run();

        // then
        assertNotEquals( 0, fsa.getFileSize( fileNames.getForVersion( 1 ) ) );
    }

    @Test
    public void shouldRecoverAndBeAbleToRotate() throws Exception
    {
        // given
        createLogFile( fsa, -1, 0, 0, -1, -1 );
        createLogFile( fsa, 10, 1, 1, 10,  0 );
        createLogFile( fsa, 20, 2, 2, 20,  1 );

        RecoveryProtocol protocol = new RecoveryProtocol( fsa, fileNames, readerPool, contentMarshal, NullLogProvider.getInstance(), 1 );

        // when
        State state = protocol.run();
        SegmentFile newFile = state.segments.rotate( 20, 20, 1 );

        // then
        assertEquals( 20, newFile.header().prevFileLastIndex() );
        assertEquals(  3, newFile.header().version() );
        assertEquals( 20, newFile.header().prevIndex() );
        assertEquals(  1, newFile.header().prevTerm() );
    }

    @Test
    public void shouldRecoverAndBeAbleToTruncate() throws Exception
    {
        // given
        createLogFile( fsa, -1, 0, 0, -1, -1 );
        createLogFile( fsa, 10, 1, 1, 10,  0 );
        createLogFile( fsa, 20, 2, 2, 20,  1 );

        RecoveryProtocol protocol = new RecoveryProtocol( fsa, fileNames, readerPool, contentMarshal, NullLogProvider.getInstance(), 1 );

        // when
        State state = protocol.run();
        SegmentFile newFile = state.segments.truncate( 20, 15, 0 );

        // then
        assertEquals( 20, newFile.header().prevFileLastIndex() );
        assertEquals(  3, newFile.header().version() );
        assertEquals( 15, newFile.header().prevIndex() );
        assertEquals(  0, newFile.header().prevTerm() );
    }

    @Test
    public void shouldRecoverAndBeAbleToSkip() throws Exception
    {
        // given
        createLogFile( fsa, -1, 0, 0, -1, -1 );
        createLogFile( fsa, 10, 1, 1, 10,  0 );
        createLogFile( fsa, 20, 2, 2, 20,  1 );

        RecoveryProtocol protocol = new RecoveryProtocol( fsa, fileNames, readerPool, contentMarshal, NullLogProvider.getInstance(), 1 );

        // when
        State state = protocol.run();
        SegmentFile newFile = state.segments.skip( 20, 40, 2 );

        // then
        assertEquals( 20, newFile.header().prevFileLastIndex() );
        assertEquals(  3, newFile.header().version() );
        assertEquals( 40, newFile.header().prevIndex() );
        assertEquals(  2, newFile.header().prevTerm() );
    }

    @Test
    public void shouldRecoverAllTermsWhenTruncatingSingleEntry() throws Exception
    {
        // given
        createLogFileWithContent( fsa, -1, 0, 0, -1, -1, 10, 0 ); //  0-- 9 @ term 0
        createLogFileWithContent( fsa,  9, 1, 1,  9,  0, 10, 1 ); // 10--19 @ term 1
        createLogFileWithContent( fsa, 19, 2, 2, 18,  1, 10, 2 ); // 19--28 @ term 2 // truncate and overwrite 1 entry (19)
        createLogFileWithContent( fsa, 28, 3, 3, 28,  2, 10, 3 ); // 29--38 @ term 3

        RecoveryProtocol protocol = new RecoveryProtocol( fsa, fileNames, readerPool, contentMarshal, NullLogProvider.getInstance(), 4 );

        // when
        State state = protocol.run();

        // then
        assertEquals( -1 , state.prevIndex );
        assertEquals( -1 , state.prevTerm );
        assertEquals( 38 , state.appendIndex );

        assertTermInRange( state.terms,  0,  9, 0 );
        assertTermInRange( state.terms, 10, 18, 1 );
        assertTermInRange( state.terms, 19, 28, 2 );
        assertTermInRange( state.terms, 29, 38, 3 );
    }

    @Test
    public void shouldRecoverAllTermsWhenTruncatingSeveralEntries() throws Exception
    {
        // given
        createLogFileWithContent( fsa, -1, 0, 0, -1, -1, 10, 0 ); //  0-- 9 @ term 0
        createLogFileWithContent( fsa,  9, 1, 1,  9,  0, 10, 1 ); // 10--19 @ term 1
        createLogFileWithContent( fsa, 19, 2, 2, 14,  1, 10, 2 ); // 15--24 @ term 2 // truncate and overwrite 5 entries (15-19)
        createLogFileWithContent( fsa, 24, 3, 3, 24,  2, 10, 3 ); // 25--34 @ term 3

        RecoveryProtocol protocol = new RecoveryProtocol( fsa, fileNames, readerPool, contentMarshal, NullLogProvider.getInstance(), 4 );

        // when
        State state = protocol.run();

        // then
        assertEquals( -1 , state.prevIndex );
        assertEquals( -1 , state.prevTerm );
        assertEquals( 34 , state.appendIndex );

        assertTermInRange( state.terms,  0,  9, 0 );
        assertTermInRange( state.terms, 10, 14, 1 );
        assertTermInRange( state.terms, 15, 24, 2 );
        assertTermInRange( state.terms, 25, 34, 3 );
    }

    // TODO: This must be handled for ALL segment files. Skips must skip the prevIndex/Term!
    @Test
    public void shouldHandleSkipDuringRecovery() throws Exception
    {
        // given
        createLogFileWithContent( fsa, -1, 0, 0, -1, -1, 10, 0 ); //  0-- 9 @ term 0
        createLogFileWithContent( fsa,  9, 1, 1,  9,  0, 10, 1 ); // 10--19 @ term 1
        createLogFileWithContent( fsa, 19, 2, 2, 28,  2, 10, 3 ); // 29--38 @ term 3 // skip to (index=28,term=2)

        RecoveryProtocol protocol = new RecoveryProtocol( fsa, fileNames, readerPool, contentMarshal, NullLogProvider.getInstance(), 3 );

        // when
        State state = protocol.run();

        // then
        assertEquals( 28 , state.prevIndex );
        assertEquals( 2 , state.prevTerm );
        assertEquals( 38 , state.appendIndex );

        assertTermInRange( state.terms,  -1, 27, 0 );
        assertTermInRange( state.terms, 28, 38, 3 );
    }

    private void assertTermInRange( Terms terms, long from, long to, long term )
    {
        for ( long index = from; index <= to; index++ )
        {
            assertEquals( "For index: " + index, term, terms.get( index ) );
        }
    }

    private void createLogFile( EphemeralFileSystemAbstraction fsa, long prevFileLastIndex, long fileNameVersion,
            long headerVersion, long prevIndex, long prevTerm ) throws IOException
    {
        StoreChannel channel = fsa.open( fileNames.getForVersion( fileNameVersion ), "w" );
        PhysicalFlushableChannel writer = new PhysicalFlushableChannel( channel );
        headerMarshal.marshal( new SegmentHeader( prevFileLastIndex, headerVersion, prevIndex, prevTerm ), writer );
        writer.prepareForFlush().flush();
        channel.close();
    }

    private void createLogFileWithContent( EphemeralFileSystemAbstraction fsa, long prevFileLastIndex, long fileNameVersion,
            long headerVersion, long prevIndex, long prevTerm, int entryCount, int entryTerm ) throws IOException
    {
        SegmentHeader header = new SegmentHeader( prevFileLastIndex, headerVersion, prevIndex, prevTerm );
        SegmentFile segment = SegmentFile.create( fsa, fileNames.getForVersion( fileNameVersion ), readerPool, fileNameVersion,
                contentMarshal, NullLogProvider.getInstance(), header );

        for ( int i = 0; i < entryCount; i++ )
        {
            segment.write( prevIndex + 1 + i, new RaftLogEntry( entryTerm , ReplicatedInteger.valueOf( i ) ) );
        }

        segment.close();
    }

    private void createEmptyLogFile( EphemeralFileSystemAbstraction fsa, long fileNameVersion ) throws IOException
    {
        StoreChannel channel = fsa.open( fileNames.getForVersion( fileNameVersion ), "w" );
        channel.close();
    }
}
