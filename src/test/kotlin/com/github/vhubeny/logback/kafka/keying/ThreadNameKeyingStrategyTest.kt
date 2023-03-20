package com.github.vhubeny.logback.kafka.keying

import ch.qos.logback.classic.Level
import ch.qos.logback.classic.LoggerContext
import ch.qos.logback.classic.spi.ILoggingEvent
import ch.qos.logback.classic.spi.LoggingEvent
import org.hamcrest.Matchers
import org.junit.Assert
import org.junit.Test
import java.nio.ByteBuffer

class ThreadNameKeyingStrategyTest {
    private val unit = ThreadNameKeyingStrategy()
    private val ctx = LoggerContext()
    @Test
    fun shouldPartitionByEventThreadName() {
        val threadName = Thread.currentThread().name
        val evt: ILoggingEvent = LoggingEvent("fqcn", ctx.getLogger("logger"), Level.ALL, "msg", null, arrayOfNulls(0))
        Assert.assertThat(
            unit.createKey(evt),
            Matchers.equalTo(ByteBuffer.allocate(4).putInt(threadName.hashCode()).array())
        )
    }
}
