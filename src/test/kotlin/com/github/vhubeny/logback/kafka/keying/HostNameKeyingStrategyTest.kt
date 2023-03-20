package com.github.vhubeny.logback.kafka.keying

import ch.qos.logback.classic.Level
import ch.qos.logback.classic.LoggerContext
import ch.qos.logback.classic.spi.ILoggingEvent
import ch.qos.logback.classic.spi.LoggingEvent
import ch.qos.logback.core.CoreConstants
import org.hamcrest.Matchers
import org.junit.Assert
import org.junit.Test
import java.nio.ByteBuffer

class HostNameKeyingStrategyTest {
    private val unit = HostNameKeyingStrategy()
    private val ctx = LoggerContext()
    @Test
    fun shouldPartitionByHostName() {
        ctx.putProperty(CoreConstants.HOSTNAME_KEY, "localhost")
        unit.context = ctx
        val evt: ILoggingEvent = LoggingEvent("fqcn", ctx.getLogger("logger"), Level.ALL, "msg", null, arrayOfNulls(0))
        Assert.assertThat(
            unit.createKey(evt),
            Matchers.equalTo(ByteBuffer.allocate(4).putInt("localhost".hashCode()).array())
        )
    }
}
