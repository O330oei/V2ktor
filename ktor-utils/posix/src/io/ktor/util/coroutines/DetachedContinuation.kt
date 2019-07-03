/*
 * Copyright 2014-2019 JetBrains s.r.o and contributors. Use of this source code is governed by the Apache 2.0 license.
 */

package io.ktor.util.coroutines

import kotlinx.cinterop.*
import kotlin.coroutines.*
import kotlin.native.concurrent.*

internal class DetachedContinuation<T>(
    origin: Continuation<T>,
    private val queue: WorkerHelpingQueue
) : Continuation<T> {
    override val context: CoroutineContext = EmptyCoroutineContext
    private val origin: COpaquePointer = StableRef.create(origin).asCPointer()

    init {
        freeze()
    }

    override fun resumeWith(result: Result<T>) {
        queue.dispatch(origin, result as Result<Any>)
    }
}
