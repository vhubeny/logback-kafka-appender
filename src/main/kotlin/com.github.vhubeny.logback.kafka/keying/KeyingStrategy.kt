package com.github.vhubeny.logback.kafka.keying

import ch.qos.logback.classic.spi.ILoggingEvent

/**
 * A strategy that can create byte array key for a given [ILoggingEvent].
 * @since 0.0.1
 */
interface KeyingStrategy<E> {
    /**
     * creates a byte array key for the given [ch.qos.logback.classic.spi.ILoggingEvent]
     * @param e the logging event
     * @return a key
     */
    fun createKey(e: E): ByteArray?
}
