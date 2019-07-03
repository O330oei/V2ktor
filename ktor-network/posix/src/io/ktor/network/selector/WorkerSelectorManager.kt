/*
 * Copyright 2014-2019 JetBrains s.r.o and contributors. Use of this source code is governed by the Apache 2.0 license.
 */
package io.ktor.network.selector

import io.ktor.util.collections.*
import io.ktor.util.coroutines.*
import kotlinx.coroutines.*
import kotlin.coroutines.*
import kotlin.native.concurrent.*

class WorkerSelectorManager : SelectorManager {
    override val coroutineContext: CoroutineContext = Dispatchers.Unconfined
    override fun notifyClosed(selectable: Selectable) {}

    private val selectorWorker = Worker.start(errorReporting = true)
    private val events: LockFreeMPSCQueue<EventInfo> = LockFreeMPSCQueue()

    init {
        freeze()
        selectorWorker.execute(TransferMode.SAFE, { events }) { events -> selectHelper(events) }
    }

    override suspend fun select(
        selectable: Selectable,
        interest: SelectInterest
    ): Unit {
        if (events.isClosed) return

        return suspendNativeCoroutine { continuation ->
            require(selectable is SelectableNative)

            val selectorState = EventInfo(selectable.descriptor, interest, continuation).freeze()
            if (!events.addLast(selectorState)) {
                continuation.resumeWithException(CancellationException("Socked closed."))
            }
        }
    }

    override fun close() {
        events.close()
        selectorWorker.requestTermination(processScheduledJobs = true)
    }
}
