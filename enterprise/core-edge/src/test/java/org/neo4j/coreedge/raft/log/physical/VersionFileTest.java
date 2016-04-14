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

import org.junit.Test;

import org.neo4j.coreedge.raft.log.DamagedLogStorageException;

import static junit.framework.TestCase.assertSame;
import static org.junit.Assert.fail;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class VersionFileTest
{
//    @Test
//    public void shouldThrowAnExceptionIfHeaderContentsDoNotMatchFile() throws Exception
//    {
//        // given
//        HeaderReader headerReader = mock( HeaderReader.class );
//        when( headerReader.readHeader( new File( "v2" ) ) ).thenReturn( HeaderBuilder.version( 3 ).prevIndex( -1 )
//                .prevTerm( -1 ) );
//
//        StoreChannelPool file = new StoreChannelPool( 2, new File( "v2" ), HeaderReader.HEADER_LENGTH, headerReader );
//
//        // when
//        try
//        {
//            file.header();
//            fail( "Should have thrown exception" );
//        }
//        catch ( DamagedLogStorageException e )
//        {
//            // then expected
//        }
//
//    }
//
//    @Test
//    public void shouldReturnHeader() throws Exception
//    {
//        // given
//        HeaderReader headerReader = mock( HeaderReader.class );
//        SegmentHeader expectedHeader = HeaderBuilder.version( 2 ).prevIndex( -1 ).prevTerm( -1 );
//        when( headerReader.readHeader( new File( "v2" ) ) ).thenReturn( expectedHeader );
//
//        StoreChannelPool file = new StoreChannelPool( 2, new File( "v2" ), HeaderReader.HEADER_LENGTH, headerReader );
//
//        // when
//        SegmentHeader header = file.header();
//
//        // then
//        assertSame( expectedHeader, header );
//    }
}
