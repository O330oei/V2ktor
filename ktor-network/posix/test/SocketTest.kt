package io.ktor.network.tests

import io.ktor.network.selector.*
import io.ktor.network.sockets.*
import io.ktor.util.coroutines.*
import kotlinx.coroutines.*
import kotlinx.coroutines.io.*
import kotlinx.io.core.*
import kotlin.coroutines.*
import kotlin.test.*


class SocketTest {

    @Test
    fun testEcho() = runBlockingWithHelp {
        WorkerSelectorManager().use { selector ->
            val tcp = aSocket(selector).tcp()
            val server = tcp.bind("127.0.0.1", 8080)

            val serverConnectionPromise = async {
                server.accept()
            }

            val clientConnection = tcp.connect("127.0.0.1", 8080)
            val serverConnection = serverConnectionPromise.await()

            val clientOutput = clientConnection.openWriteChannel()
            try {
                clientOutput.writeStringUtf8("Hello, world\n")
                clientOutput.flush()
            } finally {
                clientOutput.close()
            }

            val serverInput = serverConnection.openReadChannel()
            val message = serverInput.readUTF8Line()
            assertEquals("Hello, world", message)

            val serverOutput = serverConnection.openWriteChannel()
            try {
                serverOutput.writeStringUtf8("Hello From Server\n")
                serverOutput.flush()

                val clientInput = clientConnection.openReadChannel()
                val echo = clientInput.readUTF8Line()

                assertEquals("Hello From Server", echo)
            } finally {
                serverOutput.close()
            }

            serverConnection.close()
            clientConnection.close()

            server.close()
        }
    }
}
