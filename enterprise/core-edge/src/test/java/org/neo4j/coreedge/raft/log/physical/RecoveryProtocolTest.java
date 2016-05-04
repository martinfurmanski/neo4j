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

import org.junit.Before;
import org.junit.Test;

import java.io.File;
import java.io.IOException;

import org.neo4j.coreedge.raft.log.DamagedLogStorageException;
import org.neo4j.coreedge.raft.log.DummyRaftableContentSerializer;
import org.neo4j.coreedge.raft.replication.ReplicatedContent;
import org.neo4j.coreedge.raft.state.ChannelMarshal;
import org.neo4j.graphdb.mockfs.EphemeralFileSystemAbstraction;
import org.neo4j.io.fs.StoreChannel;
import org.neo4j.kernel.impl.transaction.log.PhysicalFlushableChannel;
import org.neo4j.logging.NullLogProvider;

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

    @Before
    public void setup()
    {
        fsa.mkdirs( root );
    }

    @Test
    public void shouldReturnEmptyStateOnEmptyDirectory() throws Exception
    {
        // given
        RecoveryProtocol protocol = new RecoveryProtocol( fsa, fileNames, contentMarshal, NullLogProvider.getInstance() );

        // when
        State state = protocol.run();

        // then
        assertEquals( -1, state.appendIndex );
        assertEquals( -1, state.currentTerm );
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

        RecoveryProtocol protocol = new RecoveryProtocol( fsa, fileNames, contentMarshal, NullLogProvider.getInstance() );

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
    public void shouldFailifTheVersionNumberInTheHeaderAndFileNameDiffers() throws Exception
    {
        // given
        createLogFile( fsa, -1, 0, 1, -1, -1 );

        RecoveryProtocol protocol = new RecoveryProtocol( fsa, fileNames, contentMarshal, NullLogProvider.getInstance() );

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

        RecoveryProtocol protocol = new RecoveryProtocol( fsa, fileNames, contentMarshal, NullLogProvider.getInstance() );

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

        RecoveryProtocol protocol = new RecoveryProtocol( fsa, fileNames, contentMarshal, NullLogProvider.getInstance() );

        // when
        protocol.run();

        // then
        assertNotEquals( 0, fsa.getFileSize( fileNames.getForVersion( 1 ) ) );
    }

    private void createLogFile( EphemeralFileSystemAbstraction fsa, long prevFileLastIndex, long fileNameVersion, long headerVersion, long prevIndex, long prevTerm ) throws IOException
    {
        StoreChannel channel = fsa.open( fileNames.getForVersion( fileNameVersion ), "w" );
        PhysicalFlushableChannel writer = new PhysicalFlushableChannel( channel );
        headerMarshal.marshal( new SegmentHeader( prevFileLastIndex, headerVersion, prevIndex, prevTerm ), writer );
        writer.prepareForFlush().flush();
        channel.close();
    }

    private void createEmptyLogFile( EphemeralFileSystemAbstraction fsa, long fileNameVersion ) throws IOException
    {
        StoreChannel channel = fsa.open( fileNames.getForVersion( fileNameVersion ), "w" );
        channel.close();
    }
}
