package io.ktor.network.util

import java.net.*

actual typealias NetworkAddress = InetSocketAddress

actual val NetworkAddress.hostname: String
    get() = hostName

actual val NetworkAddress.port: Int
    get() = port

actual typealias UnresolvedAddressException = java.nio.channels.UnresolvedAddressException

actual val NetworkAddress.isResolved: Boolean
    get() = TODO("not implemented") //To change initializer of created properties use File | Settings | File Templates.
