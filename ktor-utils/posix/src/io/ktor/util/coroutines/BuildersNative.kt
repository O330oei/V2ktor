/*
 * Copyright 2014-2019 JetBrains s.r.o and contributors. Use of this source code is governed by the Apache 2.0 license.
 */
@file:Suppress("INVISIBLE_MEMBER", "INVISIBLE_REFERENCE")

package io.ktor.util.coroutines

import io.ktor.util.*
import kotlinx.coroutines.*
import kotlin.coroutines.*

@UseExperimental(InternalCoroutinesApi::class)
@InternalAPI
fun <T> runBlockingWithHelp(
    block: suspend CoroutineScope.() -> T
): T = runBlocking {
    val loop = coroutineContext[ContinuationInterceptor] as EventLoop

    val blockingTask = async(Dispatchers.Unconfined + WorkerHelpingQueue.current) {
        block()
    }

    try {
        loop.incrementUseCount()

        while (true) {
            WorkerHelpingQueue.current.processEvents {
                loop.dispatch(coroutineContext, it)
            }

            loop.processNextEvent()
            if (blockingTask.isCompleted) break
        }

        return@runBlocking blockingTask.getCompleted()
    } finally {
        loop.decrementUseCount()
    }
}

@InternalAPI
suspend fun <T> suspendNativeCoroutine(block: (Continuation<T>) -> Unit): T = suspendCancellableCoroutine {
    val queue = WorkerHelpingQueue.current
    block(DetachedContinuation(it, queue))
}
