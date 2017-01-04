/*
 * Copyright (c) 2002-2017 "Neo Technology,"
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
package org.neo4j.causalclustering.catchup.storecopy;

import java.io.IOException;
import java.io.OutputStream;
import java.util.concurrent.CompletableFuture;

import org.neo4j.causalclustering.catchup.CoreClient;
import org.neo4j.causalclustering.catchup.CoreCommunicationException;
import org.neo4j.causalclustering.catchup.CoreClientResponseAdaptor;
import org.neo4j.causalclustering.identity.MemberId;
import org.neo4j.causalclustering.identity.StoreId;
import org.neo4j.logging.Log;
import org.neo4j.logging.LogProvider;

public class StoreCopyClient
{
    private final CoreClient coreClient;
    private final Log log;

    public StoreCopyClient( CoreClient coreClient, LogProvider logProvider )
    {
        this.coreClient = coreClient;
        log = logProvider.getLog( getClass() );
    }

    long copyStoreFiles( MemberId from, StoreId expectedStoreId, StoreFileStreams storeFileStreams ) throws StoreCopyFailedException
    {
        try
        {
            return coreClient.makeBlockingRequest( from, new GetStoreRequest( expectedStoreId ),
                    new CoreClientResponseAdaptor<Long>()
                    {
                        private String destination;

                        @Override
                        public void onFileHeader( CompletableFuture<Long> requestOutcomeSignal, FileHeader fileHeader )
                        {
                            this.destination = fileHeader.fileName();
                        }

                        @Override
                        public boolean onFileContent( CompletableFuture<Long> signal, FileChunk fileChunk )
                                throws IOException
                        {
                            try ( OutputStream outputStream = storeFileStreams.createStream( destination ) )
                            {
                                outputStream.write( fileChunk.bytes() );
                            }
                            return fileChunk.isLast();
                        }

                        @Override
                        public void onFileStreamingComplete( CompletableFuture<Long> signal,
                                                             StoreCopyFinishedResponse response )
                        {
                            log.info( "Finished streaming %s", destination );
                            signal.complete( response.lastCommittedTxBeforeStoreCopy() );
                        }
                    } );
        }
        catch ( CoreCommunicationException e )
        {
            throw new StoreCopyFailedException( e );
        }
    }

    StoreId fetchStoreId( MemberId from ) throws StoreIdDownloadFailedException
    {
        try
        {
            return coreClient.makeBlockingRequest( from, new GetStoreIdRequest(),
                    new CoreClientResponseAdaptor<StoreId>()
                    {
                        @Override
                        public void onGetStoreIdResponse( CompletableFuture<StoreId> signal,
                                                          GetStoreIdResponse response )
                        {
                            signal.complete( response.storeId() );
                        }
                    } );
        }
        catch ( CoreCommunicationException e )
        {
            throw new StoreIdDownloadFailedException( e );
        }
    }
}
