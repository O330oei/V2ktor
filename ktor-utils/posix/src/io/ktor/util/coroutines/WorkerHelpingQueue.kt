/*
 * Copyright 2014-2019 JetBrains s.r.o and contributors. Use of this source code is governed by the Apache 2.0 license.
 */

package io.ktor.util.coroutines

import io.ktor.util.collections.*
import kotlinx.cinterop.*
import kotlinx.coroutines.*
import kotlin.coroutines.*
import kotlin.native.ThreadLocal
import kotlin.native.concurrent.*

internal class WorkerHelpingQueue : CoroutineContext.Element {
    override val key: CoroutineContext.Key<*> get() = Key
    private val tasks = LockFreeMPSCQueue<Task>()

    init {
        freeze()
    }

    /**
     * Dispatch [callback].
     */
    fun dispatch(callback: COpaquePointer, result: Result<Any>) {
        val task = Task(callback, result)
        tasks.addLast(task)
    }

    /**
     * Process [callback] in place.
     */
    fun processEvents(block: (Runnable) -> Unit) {
        while (true) {
            val task = tasks.removeFirstOrNull() ?: return
            val callback = task.callback.asStableRef<Continuation<in Any>>()

            block(Runnable {
                try {
                    callback.get().resumeWith(task.result as Result<Any>)
                } finally {
                    callback.dispose()
                }
            })
        }
    }


    companion object Key : CoroutineContext.Key<WorkerHelpingQueue> {
        @ThreadLocal
        val current = WorkerHelpingQueue()
    }
}

private class Task(val callback: COpaquePointer, val result: Any?) {
    init {
        freeze()
    }
}
