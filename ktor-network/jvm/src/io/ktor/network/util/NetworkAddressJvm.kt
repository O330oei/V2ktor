package io.ktor.network.util

import java.net.*

actual typealias NetworkAddress = InetSocketAddress

actual val NetworkAddress.hostname: String
    get() = hostName

actual val NetworkAddress.port: Int
    get() = port
