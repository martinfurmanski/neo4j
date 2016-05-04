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

import static org.junit.Assert.assertEquals;
import static org.mockito.Matchers.any;
import static org.mockito.Mockito.RETURNS_MOCKS;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.util.TreeMap;

import org.junit.Test;
import org.neo4j.coreedge.raft.replication.ReplicatedContent;
import org.neo4j.coreedge.raft.state.ChannelMarshal;
import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.logging.LogProvider;

public class RecoveryProtocolTest
{
    @Test
    public void shouldReturnEmptyStateOnEmptyDirectory() throws Exception
    {
        // Given
        FileSystemAbstraction fsa = mock( FileSystemAbstraction.class, RETURNS_MOCKS );
        FileNames fileNames = mock( FileNames.class );
        ChannelMarshal<ReplicatedContent> contentMarshal = mock( ChannelMarshal.class );
        LogProvider logProvider = mock( LogProvider.class );
        RecoveryProtocol protocol = new RecoveryProtocol( fsa, fileNames, contentMarshal, logProvider );

        when( fileNames.getAllFiles( any(), any() ) ).thenReturn( new TreeMap<>() );

        // When
        State state = protocol.run();

        // Then
        assertEquals( -1, state.appendIndex );
        assertEquals( -1, state.currentTerm );
        assertEquals( -1, state.prevIndex );
        assertEquals( -1, state.prevTerm );
        assertEquals( 0, state.segments.last().header().version() );
    }


}
