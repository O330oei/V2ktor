/*
 * Copyright 2014-2019 JetBrains s.r.o and contributors. Use of this source code is governed by the Apache 2.0 license.
 */

package io.ktor.network.selector

import io.ktor.util.*
import kotlin.coroutines.*

@InternalAPI
actual fun platformSelectorManager(dispatcher: CoroutineContext): SelectorManager = ActorSelectorManager(dispatcher)
