/*
 * Copyright 2014-2019 JetBrains s.r.o and contributors. Use of this source code is governed by the Apache 2.0 license.
 */

package io.ktor.network.sockets

import io.ktor.network.selector.*
import io.ktor.network.util.*
import java.net.*

suspend fun TCPSocketBuilder.connect(
    remoteAddress: SocketAddress,
    configure: SocketOptions.TCPClientSocketOptions.() -> Unit = {}
): Socket = connect(remoteAddress as NetworkAddress, configure)


internal actual suspend fun TCPSocketBuilder.Companion.connect(
    selector: SelectorManager,
    networkAddress: NetworkAddress,
    socketOptions: SocketOptions.TCPClientSocketOptions
): Socket = selector.buildOrClose({ openSocketChannel() }) {
    assignOptions(socketOptions)
    nonBlocking()

    SocketImpl(this, socket()!!, selector, socketOptions).apply {
        connect(networkAddress)
    }
}

internal actual fun TCPSocketBuilder.Companion.bind(
    selector: SelectorManager,
    localAddress: NetworkAddress?,
    socketOptions: SocketOptions.AcceptorOptions
): ServerSocket = selector.buildOrClose({ openServerSocketChannel() }) {
    assignOptions(socketOptions)
    nonBlocking()

    ServerSocketImpl(this, selector).apply {
        channel.socket().bind(localAddress)
    }
}
