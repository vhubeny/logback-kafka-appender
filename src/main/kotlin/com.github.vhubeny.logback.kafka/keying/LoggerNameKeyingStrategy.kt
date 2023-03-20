package com.github.vhubeny.logback.kafka.keying

import ch.qos.logback.classic.spi.ILoggingEvent
import java.nio.ByteBuffer

/**
 * This strategy uses the logger name as partitioning key. This ensures that all messages logged by the
 * same logger will remain in the correct order for any consumer.
 * But this strategy can lead to uneven log distribution for a small number of distinct loggers (compared to the number of partitions).
 */
class LoggerNameKeyingStrategy : KeyingStrategy<ILoggingEvent> {
    override fun createKey(e: ILoggingEvent): ByteArray {
        val loggerName: String = if (e.loggerName == null) {
            ""
        } else {
            e.loggerName
        }
        return ByteBuffer.allocate(4).putInt(loggerName.hashCode()).array()
    }
}
