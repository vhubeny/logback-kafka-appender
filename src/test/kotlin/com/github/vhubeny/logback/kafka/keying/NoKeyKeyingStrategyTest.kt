package com.github.vhubeny.logback.kafka.keying

import ch.qos.logback.classic.spi.ILoggingEvent
import org.hamcrest.Matchers
import org.junit.Assert
import org.junit.Test
import org.mockito.Mockito

class NoKeyKeyingStrategyTest {
    private val unit = NoKeyKeyingStrategy()
    @Test
    fun shouldAlwaysReturnNull() {
        Assert.assertThat(
            unit.createKey(
                Mockito.mock(
                    ILoggingEvent::class.java
                )
            ), Matchers.`is`(Matchers.nullValue())
        )
    }
}
