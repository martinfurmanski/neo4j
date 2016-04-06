package org.neo4j.coreedge.discovery.simple;

import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.MessageToMessageEncoder;

import java.util.List;

class DiscoveryGossipEncoder extends MessageToMessageEncoder<DiscoveryGossip>
{
    @Override
    protected void encode( ChannelHandlerContext ctx, DiscoveryGossip gossip, List<Object> out ) throws Exception
    {
        ByteBuf buffer = ctx.alloc().buffer();
        gossip.serialize( buffer );
        out.add( buffer );
    }
}
