package io.ktor.network.selector

import io.ktor.util.*
import kotlinx.coroutines.*
import kotlinx.io.core.*

/**
 * A selectable entity with selectable NIO [channel], [interestedOps] subscriptions
 */
@KtorExperimentalAPI
expect interface Selectable

@Suppress("KDocMissingDocumentation")
class ClosedChannelCancellationException : CancellationException("Closed channel.")
