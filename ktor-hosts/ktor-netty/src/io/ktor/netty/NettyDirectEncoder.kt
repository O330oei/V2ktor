package io.ktor.netty

import io.netty.buffer.*
import io.netty.channel.*
import io.netty.handler.codec.*
import io.netty.handler.codec.http.*

internal class NettyDirectEncoder : MessageToByteEncoder<HttpContent>() {
    override fun encode(ctx: ChannelHandlerContext, msg: HttpContent, out: ByteBuf) {
        out.writeBytes(msg.content())
    }

    override fun allocateBuffer(ctx: ChannelHandlerContext, msg: HttpContent?, preferDirect: Boolean): ByteBuf {
        val size = msg?.content()?.readableBytes() ?: 0
        return if (size == 0) Unpooled.EMPTY_BUFFER
        else if (preferDirect) ctx.alloc().ioBuffer(size)
        else ctx.alloc().heapBuffer(size)
    }
}
