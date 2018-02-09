/*
 * Copyright (c) 2002-2018 "Neo Technology,"
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
package org.neo4j.causalclustering.protocol;

import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelDuplexHandler;
import io.netty.channel.ChannelFutureListener;
import io.netty.channel.ChannelHandler;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelOutboundHandlerAdapter;
import io.netty.channel.ChannelPipeline;
import io.netty.channel.ChannelPromise;
import io.netty.handler.codec.ByteToMessageDecoder;
import io.netty.handler.codec.LengthFieldBasedFrameDecoder;
import io.netty.handler.codec.LengthFieldPrepender;
import io.netty.handler.codec.MessageToByteEncoder;

import java.util.ArrayList;
import java.util.List;

import org.neo4j.logging.Log;

import static java.util.Arrays.asList;

/**
 * Builder and installer of pipelines.
 * <p>
 * Makes sures to install sane last-resort error handling and
 * handles the construction of common patterns, like framing.
 * <p>
 * Do not modify the names of handlers you install.
 */
public class NettyPipelineBuilder
{
    private static final String FIXED_NAME = "fixed";
    private static final String NOT_FIXED_NAME = "not-fixed";

    private final ChannelPipeline pipeline;
    private final Log log;
    private final List<HandlerInfo> handlers = new ArrayList<>();

    private NettyPipelineBuilder( ChannelPipeline pipeline, Log log )
    {
        this.pipeline = pipeline;
        this.log = log;
    }

    /**
     * Entry point for the builder.
     *
     * @param pipeline The pipeline to build for.
     * @param log The log used for last-resort errors occurring in the pipeline.
     * @return The builder.
     */
    public static NettyPipelineBuilder with( ChannelPipeline pipeline, Log log )
    {
        return new NettyPipelineBuilder( pipeline, log );
    }

    /**
     * Adds buffer framing to the pipeline. Useful for pipelines marshalling
     * complete POJOs as an example using {@link MessageToByteEncoder} and
     * {@link ByteToMessageDecoder}.
     */
    public NettyPipelineBuilder addFraming( boolean fixed )
    {
        add( "framing_decoder", fixed, new LengthFieldBasedFrameDecoder( Integer.MAX_VALUE, 0, 4, 0, 4 ) );
        add( "framing_encoder", fixed, new LengthFieldPrepender( 4 ) );
        return this;
    }

    /**
     * @see #addFraming(boolean)
     */
    public NettyPipelineBuilder addFraming()
    {
        return addFraming( false );
    }

    /**
     * Adds handlers to the pipeline.
     * <p>
     * The pipeline builder controls the internal names of the handlers in the
     * pipeline and external actors are forbidden from manipulating them.
     *
     * @param name The name of the handler, which must be unique.
     * @param fixed Fixed handlers will not be removed on a subsequent re-install.
     * @param newHandlers The new handlers.
     * @return The builder.
     */
    public NettyPipelineBuilder add( String name, boolean fixed, List<ChannelHandler> newHandlers )
    {
        newHandlers.stream().map( handler -> new HandlerInfo( name, fixed, handler ) ).forEachOrdered( handlers::add );
        return this;
    }

    /**
     * @see #add(String, boolean, List)
     */
    public NettyPipelineBuilder add( String name, boolean fixed, ChannelHandler... newHandlers )
    {
        return add( name, fixed, asList( newHandlers ) );
    }

    /**
     * @see #add(String, boolean, List)
     */
    public NettyPipelineBuilder addFixed( String name, ChannelHandler... newHandlers )
    {
        return add( name, true, asList( newHandlers ) );
    }

    /**
     * @see #add(String, boolean, List)
     */
    public NettyPipelineBuilder add( String name, ChannelHandler... newHandlers )
    {
        return add( name, false, asList( newHandlers ) );
    }

    /**
     * Installs the built pipeline and removes any old pipeline,
     * except for those previously installed as fixed.
     */
    public void install()
    {
        clear();
        for ( HandlerInfo hi : handlers )
        {
            pipeline.addLast( nameOf( hi ), hi.handler );
        }
        installErrorHandling();
    }

    private String nameOf( HandlerInfo info )
    {
        return (info.fixed ? FIXED_NAME : NOT_FIXED_NAME) + info.name;
    }

    private void clear()
    {
        pipeline.names().stream().filter( this::isNotDefault ).filter( this::isNotFixed ).forEach( pipeline::remove );
    }

    private boolean isNotFixed( String name )
    {
        return !name.startsWith( FIXED_NAME );
    }

    private boolean isNotDefault( String name )
    {
        // these are netty internal handlers for head and tail
        return pipeline.get( name ) != null;
    }

    private void installErrorHandling()
    {
        // inbound goes in the direction from first->last
        pipeline.addLast( "error_handler_tail", new ChannelDuplexHandler()
        {
            @Override
            public void exceptionCaught( ChannelHandlerContext ctx, Throwable cause )
            {
                log.error( "Exception in inbound", cause );
            }

            @Override
            public void channelRead( ChannelHandlerContext ctx, Object msg )
            {
                log.error( "Unhandled inbound message: " + msg );
            }

            // this is the first handler for an outbound message, and attaches a listener to its promise if possible
            @Override
            public void write( ChannelHandlerContext ctx, Object msg, ChannelPromise promise )
            {
                // if the promise is a void-promise, then exceptions will instead propagate to the
                // exceptionCaught handler on the outbound handler further below

                if ( !promise.isVoid() )
                {
                    promise.addListener( (ChannelFutureListener) future -> {
                        if ( !future.isSuccess() )
                        {
                            log.error( "Exception in outbound", future.cause() );
                        }
                    } );
                }
                ctx.write( msg, promise );
            }
        } );

        pipeline.addFirst( "error_handler_head", new ChannelOutboundHandlerAdapter()
        {
            // exceptions which did not get fulfilled on the promise of a write, etc.
            @Override
            public void exceptionCaught( ChannelHandlerContext ctx, Throwable cause )
            {
                log.error( "Exception in outbound", cause );
            }

            // netty can only handle bytes in the form of ByteBuf, so if you reach this then you are
            // perhaps trying to send a POJO without having a suitable encoder
            @Override
            public void write( ChannelHandlerContext ctx, Object msg, ChannelPromise promise )
            {
                if ( !(msg instanceof ByteBuf) )
                {
                    log.error( "Unhandled outbound message: " + msg );
                }
                else
                {
                    ctx.write( msg, promise );
                }
            }
        } );
    }

    private static class HandlerInfo
    {
        private final String name;
        private final boolean fixed;
        private final ChannelHandler handler;

        HandlerInfo( String name, boolean fixed, ChannelHandler handler )
        {
            this.name = name;
            this.fixed = fixed;
            this.handler = handler;
        }
    }
}
