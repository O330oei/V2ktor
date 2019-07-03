/*
 * Copyright 2014-2020 JetBrains s.r.o and contributors. Use of this source code is governed by the Apache 2.0 license.
 */

package io.ktor.network.sockets

import io.ktor.network.selector.*
import io.ktor.network.util.*

internal actual suspend fun TCPSocketBuilder.Companion.connect(
    selector: SelectorManager,
    networkAddress: NetworkAddress,
    socketOptions: SocketOptions.TCPClientSocketOptions
): Socket {
    TODO("not implemented") //To change body of created functions use File | Settings | File Templates.
}

internal actual fun TCPSocketBuilder.Companion.bind(
    selector: SelectorManager,
    localAddress: NetworkAddress?,
    socketOptions: SocketOptions.AcceptorOptions
): ServerSocket {
    TODO("not implemented") //To change body of created functions use File | Settings | File Templates.
}
