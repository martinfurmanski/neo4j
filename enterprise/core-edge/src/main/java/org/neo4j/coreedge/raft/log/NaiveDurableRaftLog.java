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
package org.neo4j.coreedge.raft.log;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import org.neo4j.coreedge.raft.replication.MarshallingException;
import org.neo4j.coreedge.raft.replication.ReplicatedContent;
import org.neo4j.coreedge.server.ByteBufMarshal;
import org.neo4j.helpers.collection.LruCache;
import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.io.fs.StoreChannel;
import org.neo4j.kernel.lifecycle.LifecycleAdapter;
import org.neo4j.logging.Log;
import org.neo4j.logging.LogProvider;

/**
 * Writes a raft log to disk using 3 files:
 * <p>
 * 1. entries.log
 * ┌─────────────────────────────┐
 * │term                  8 bytes│
 * │contentPointer        8 bytes│
 * ├─────────────────────────────┤
 * │record length        16 bytes│
 * └─────────────────────────────┘
 * <p>
 * 2. content.log
 * ┌─────────────────────────────┐
 * │contentLength         4 bytes│
 * ├─────────────────────────────┤
 * │contentType           1 bytes│
 * │content              variable│
 * ├─────────────────────────────┤
 * │record length        variable│
 * └─────────────────────────────┘
 * <p>
 * 3. commit.log
 * ┌─────────────────────────────┐
 * │committedIndex        8 bytes│
 * ├─────────────────────────────┤
 * │record length         8 bytes│
 * └─────────────────────────────┘
 */
public class NaiveDurableRaftLog extends LifecycleAdapter implements RaftLog
{
    public static final int ENTRY_RECORD_LENGTH = 16;
    public static final int CONTENT_LENGTH_BYTES = 4;
    public static final int COMMIT_INDEX_BYTES = 8;
    public static final String DIRECTORY_NAME = "raft-log";

    private final StoreChannel entriesChannel;
    private final StoreChannel contentChannel;
    private final StoreChannel commitChannel;

    private final ByteBufMarshal<ReplicatedContent> marshal;
    private final Log log;
    private long appendIndex = -1;
    private long contentOffset;
    private long commitIndex = -1;
    private long term = -1;

    LruCache<Long,RaftLogEntry> cache = new LruCache<>( "raftlog", 10  );

    public NaiveDurableRaftLog( FileSystemAbstraction fileSystem, File directory,
                                ByteBufMarshal<ReplicatedContent> marshal, LogProvider logProvider )
    {
        this.marshal = marshal;

        directory.mkdirs();

        try
        {
            entriesChannel = fileSystem.open( new File( directory, "entries.log" ), "rw" );
            contentChannel = fileSystem.open( new File( directory, "content.log" ), "rw" );
            commitChannel = fileSystem.open( new File( directory, "commit.log" ), "rw" );
            appendIndex = entriesChannel.size() / ENTRY_RECORD_LENGTH - 1;
            contentOffset = contentChannel.size();
            commitIndex = readCommitIndex();
            log = logProvider.getLog( getClass() );

            log.info( "Raft log created. AppendIndex: %d, commitIndex: %d", appendIndex, commitIndex );
        }
        catch ( IOException e )
        {
            throw new RuntimeException( e );
        }
    }

    @Override
    public void shutdown() throws Throwable
    {
        Exception container = new Exception( "Exception happened during shutdown of RaftLog. See suppressed " +
                "exceptions for details" );
        boolean shouldThrow;
        shouldThrow = forceAndCloseChannel( entriesChannel, container );
        shouldThrow = forceAndCloseChannel( contentChannel, container ) || shouldThrow;
        shouldThrow = forceAndCloseChannel( commitChannel, container ) || shouldThrow;
        if ( shouldThrow )
        {
            throw container;
        }
    }

    /**
     * This method will try to force and close a store channel. If any of these two operations fails, the exception
     * will be added as suppressed in the provided container. In such a case, true will be returned.
     *
     * @param channel   The channel to close
     * @param container The container to add supressed exceptions in the case of failure
     * @return True iff an exception was thrown by either force() or close()
     */
    private boolean forceAndCloseChannel( StoreChannel channel, Exception container )
    {
        boolean exceptionHappened = false;
        try
        {
            channel.force( false );
            channel.close();
        }
        catch ( Exception e )
        {
            exceptionHappened = true;
            container.addSuppressed( e );
        }
        return exceptionHappened;
    }

    @Override
    public long append( RaftLogEntry logEntry ) throws RaftStorageException
    {
        if ( logEntry.term() >= term )
        {
            term = logEntry.term();
        }
        else
        {
            throw new RaftStorageException( String.format( "Non-monotonic term %d for in entry %s in term %d",
                    logEntry.term(), logEntry.toString(), term ) );
        }

        try
        {
            int length = writeContent( logEntry );
            writeEntry( new Entry( logEntry.term(), contentOffset ) );
            contentOffset += length;
            appendIndex++;
            cache.put( appendIndex, logEntry );
            return appendIndex;
        }
        catch ( MarshallingException | IOException e )
        {
            throw new RaftStorageException( "Failed to append log entry", e );
        }
    }

    @Override
    public void truncate( long fromIndex ) throws RaftStorageException
    {
        cache.clear();

        try
        {
            if ( fromIndex <= commitIndex )
            {
                throw new IllegalArgumentException( "cannot truncate before the commit index" );
            }

            if ( appendIndex >= fromIndex )
            {
                Entry entry = readEntry( fromIndex );
                contentChannel.truncate( entry.contentPointer );
                contentOffset = entry.contentPointer;

                entriesChannel.truncate( ENTRY_RECORD_LENGTH * fromIndex );
                entriesChannel.force( false );

                appendIndex = fromIndex - 1;
            }
            term = readEntryTerm( appendIndex );
        }
        catch ( IOException e )
        {
            throw new RaftStorageException( "Failed to truncate", e );
        }
    }

    @Override
    public void commit( final long newCommitIndex ) throws RaftStorageException
    {
        if ( commitIndex == appendIndex )
        {
            return;
        }
        long actualNewCommitIndex = newCommitIndex;
        if ( newCommitIndex > appendIndex )
        {
            actualNewCommitIndex = appendIndex;
        }
        // INVARIANT: If newCommitIndex was greater than appendIndex, commitIndex is equal to appendIndex
        try
        {
            storeCommitIndex( actualNewCommitIndex );
        }
        catch ( IOException e )
        {
            throw new RaftStorageException( "Failed to commit", e );
        }
        commitIndex = actualNewCommitIndex;

//        while ( commitIndex < actualNewCommitIndex )
//        {
//            commitIndex++;
//            for ( Listener listener : listeners )
//            {
//                ReplicatedContent content = readEntryContent( commitIndex );
//                listener.onCommitted( content, commitIndex );
//            }
//        }
    }

    @Override
    public long appendIndex()
    {
        return appendIndex;
    }

    @Override
    public long commitIndex()
    {
        return commitIndex;
    }

    @Override
    public RaftLogEntry readLogEntry( long logIndex ) throws RaftStorageException
    {
        RaftLogEntry logEntry = cache.get( logIndex );
        if( logEntry != null )
        {
            return logEntry;
        }

        try
        {
            Entry entry = readEntry( logIndex );
            ReplicatedContent content = readContentFrom( entry.contentPointer );

            return new RaftLogEntry( entry.term, content );
        }
        catch ( IOException | MarshallingException e )
        {
            throw new RaftStorageException( "Failed to read log entry", e );
        }
    }

    @Override
    public ReplicatedContent readEntryContent( long logIndex ) throws RaftStorageException
    {
        RaftLogEntry logEntry = cache.get( logIndex );
        if( logEntry != null )
        {
            return logEntry.content();
        }

        return readLogEntry( logIndex ).content();
    }

    @Override
    public long readEntryTerm( long logIndex ) throws RaftStorageException
    {
        RaftLogEntry logEntry = cache.get( logIndex );
        if( logEntry != null )
        {
            return logEntry.term();
        }

        try
        {
            return readEntry( logIndex ).term;
        }
        catch ( IOException e )
        {
            throw new RaftStorageException( "Failed to read term", e );
        }
    }

    @Override
    public boolean entryExists( long logIndex )
    {
        return appendIndex >= logIndex;
    }

    private static class Entry
    {
        private final long term;
        private final long contentPointer;

        public Entry( long term, long contentPointer )
        {
            this.term = term;
            this.contentPointer = contentPointer;
        }

        @Override
        public String toString()
        {
            return "Entry{" +
                    "term=" + term +
                    ", contentPointer=" + contentPointer +
                    '}';
        }
    }

    private void writeEntry( Entry entry ) throws IOException
    {
        ByteBuffer buffer = ByteBuffer.allocate( ENTRY_RECORD_LENGTH );
        buffer.putLong( entry.term );
        buffer.putLong( entry.contentPointer );
        buffer.flip();

        entriesChannel.writeAll( buffer, (appendIndex + 1) * ENTRY_RECORD_LENGTH );
        entriesChannel.force( false );
    }

    private Entry readEntry( long logIndex ) throws IOException
    {
        if ( logIndex < 0 || logIndex > appendIndex )
        {
            return new Entry( -1, -1 );
        }

        ByteBuffer buffer = ByteBuffer.allocate( ENTRY_RECORD_LENGTH );
        entriesChannel.read( buffer, logIndex * ENTRY_RECORD_LENGTH );
        buffer.flip();
        long term = buffer.getLong();
        long contentPointer = buffer.getLong();
        return new Entry( term, contentPointer );
    }

    private int writeContent( RaftLogEntry logEntry ) throws MarshallingException, IOException
    {
        ByteBuf buffer = Unpooled.buffer();
        marshal.marshal( logEntry.content(), buffer );
        ByteBuffer contentBuffer = buffer.internalNioBuffer( 0, buffer.writerIndex() );
        int length = CONTENT_LENGTH_BYTES + contentBuffer.remaining();

        ByteBuffer contentLengthBuffer = ByteBuffer.allocate( CONTENT_LENGTH_BYTES );
        contentLengthBuffer.putInt( length );
        contentLengthBuffer.flip();
        contentChannel.writeAll( contentLengthBuffer, contentOffset );
        contentChannel.writeAll( contentBuffer, contentOffset + CONTENT_LENGTH_BYTES );
        contentChannel.force( false );

        return length;
    }

    private ReplicatedContent readContentFrom( long contentPointer ) throws IOException, MarshallingException
    {
        ByteBuffer lengthBuffer = ByteBuffer.allocate( CONTENT_LENGTH_BYTES );
        contentChannel.read( lengthBuffer, contentPointer );
        lengthBuffer.flip();
        int contentLength = lengthBuffer.getInt();

        ByteBuffer contentBuffer = ByteBuffer.allocate( contentLength - CONTENT_LENGTH_BYTES );
        contentChannel.read( contentBuffer, contentPointer + CONTENT_LENGTH_BYTES );
        contentBuffer.flip();
        ByteBuf byteBuf = Unpooled.wrappedBuffer( contentBuffer );
        return marshal.unmarshal( byteBuf );
    }

    private void storeCommitIndex( long commitIndex ) throws IOException
    {
        ByteBuffer buffer = ByteBuffer.allocate( COMMIT_INDEX_BYTES );
        buffer.putLong( commitIndex );
        buffer.flip();
        commitChannel.writeAll( buffer, 0 );
        commitChannel.force( false );
    }

    private long readCommitIndex() throws IOException
    {
        if ( commitChannel.size() < COMMIT_INDEX_BYTES )
        {
            return -1;
        }
        ByteBuffer buffer = ByteBuffer.allocate( COMMIT_INDEX_BYTES );
        commitChannel.read( buffer, 0 );
        buffer.flip();
        return buffer.getLong();
    }
}
