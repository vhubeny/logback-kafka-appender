package com.github.vhubeny.logback.kafka

import ch.qos.logback.classic.Level
import ch.qos.logback.classic.LoggerContext
import ch.qos.logback.classic.spi.ILoggingEvent
import ch.qos.logback.classic.spi.LoggingEvent
import ch.qos.logback.core.BasicStatusManager
import ch.qos.logback.core.encoder.Encoder
import ch.qos.logback.core.status.ErrorStatus
import com.github.vhubeny.logback.kafka.delivery.DeliveryStrategy
import com.github.vhubeny.logback.kafka.keying.KeyingStrategy
import com.nhaarman.mockitokotlin2.*
import org.apache.kafka.clients.producer.ProducerRecord
import org.hamcrest.Matchers
import org.junit.After
import org.junit.Assert
import org.junit.Before
import org.junit.Test
import org.junit.jupiter.api.extension.ExtendWith
import org.mockito.ArgumentCaptor
import org.mockito.Captor
import org.mockito.Mockito.verifyNoInteractions
import org.mockito.Mockito.`when`
import org.mockito.MockitoAnnotations.openMocks
import org.mockito.junit.jupiter.MockitoExtension


@ExtendWith(MockitoExtension::class)
class KafkaAppenderTest {
    @Captor
    private lateinit var captor: ArgumentCaptor<ProducerRecord<Any, Any>>
    private val appender = KafkaAppender<ILoggingEvent>()
    private val ctx = LoggerContext()

    private val encoder: Encoder<ILoggingEvent> = mock()
    private val keyingStrategy: KeyingStrategy<ILoggingEvent> = mock()
    private var deliveryStrategy: DeliveryStrategy = mock()

    @Before
    fun before() {
        openMocks(this);
        ctx.name = "testctx"
        ctx.statusManager = BasicStatusManager()
        appender.context = ctx
        appender.name = "kafkaAppenderBase"
        appender.encoder = encoder
        appender.topic = "topic"
        appender.addProducerConfig("bootstrap.servers=localhost:1234")
        appender.keyingStrategy = keyingStrategy
        appender.deliveryStrategy = deliveryStrategy
        ctx.start()
    }

    @After
    fun after() {
        ctx.stop()
        appender.stop()
    }

    @Test
    fun testPerfectStartAndStop() {
        appender.start()
        Assert.assertTrue("isStarted", appender.isStarted)
        appender.stop()
        Assert.assertFalse("isStopped", appender.isStarted)
        Assert.assertThat(ctx.statusManager.copyOfStatusList, Matchers.empty())
        verifyNoInteractions(encoder, keyingStrategy, deliveryStrategy)
    }

    @Test
    fun testDontStartWithoutTopic() {
        appender.topic = null
        appender.start()
        Assert.assertFalse("isStarted", appender.isStarted)
        Assert.assertThat(
            ctx.statusManager.copyOfStatusList,
            Matchers.hasItem(ErrorStatus("No topic set for the appender named [\"kafkaAppenderBase\"].", null))
        )
    }

    @Test
    fun testDontStartWithoutBootstrapServers() {
        appender.producerConfig.clear()
        appender.start()
        Assert.assertFalse("isStarted", appender.isStarted)
        Assert.assertThat(
            ctx.statusManager.copyOfStatusList,
            Matchers.hasItem(
                ErrorStatus(
                    "No \"bootstrap.servers\" set for the appender named [\"kafkaAppenderBase\"].",
                    null
                )
            )
        )
    }

    @Test
    fun testDontStartWithoutEncoder() {
        appender.encoder = null
        appender.start()
        Assert.assertFalse("isStarted", appender.isStarted)
        Assert.assertThat(
            ctx.statusManager.copyOfStatusList,
            Matchers.hasItem(ErrorStatus("No encoder set for the appender named [\"kafkaAppenderBase\"].", null))
        )
    }

    @Test
    fun testAppendUsesKeying() {
        `when`(
            encoder.encode(mock())
        ).thenReturn(byteArrayOf(0x00, 0x00))
        appender.start()
        val evt = LoggingEvent("fqcn", ctx.getLogger("logger"), Level.ALL, "message", null, arrayOfNulls(0))
        appender.append(evt)

        verifyMock(evt)
        verify(keyingStrategy).createKey(same(evt))
        verifyMock(evt)
    }

    @Test
    fun testAppendUsesPreConfiguredPartition() {
        `when`(encoder.encode(mock()))
            .thenReturn(byteArrayOf(0x00, 0x00))
        appender.partition = 1
        appender.start()
        val evt = LoggingEvent("fqcn", ctx.getLogger("logger"), Level.ALL, "message", null, arrayOfNulls(0))
        appender.append(evt)
        verifyMock(evt, true)
        val value = captor.value
        Assert.assertThat(value.partition(), Matchers.equalTo(1))
    }

    @Test
    fun testDeferredAppend() {
        `when`(encoder.encode(mock()))
            .thenReturn(byteArrayOf(0x00, 0x00))
        appender.start()
        val deferredEvent = LoggingEvent(
            "fqcn",
            ctx.getLogger("org.apache.kafka.clients.logger"),
            Level.ALL,
            "deferred message",
            null,
            arrayOfNulls(0)
        )
        appender.doAppend(deferredEvent)
        verify(deliveryStrategy, never()).send<Any, Any, Any>(
            any(),
            any(),
            eq(deferredEvent),
            any()
        )
        val evt = LoggingEvent("fqcn", ctx.getLogger("logger"), Level.ALL, "message", null, arrayOfNulls(0))
        appender.doAppend(evt)
        verifyMock(evt)
        verifyMock(evt)
    }

    private fun verifyMock(evt: LoggingEvent, capture: Boolean = false) {
        verify(deliveryStrategy).send(
            any(),
            if (capture) captor.capture() else any(),
            eq(evt),
            any()
        )
    }

    @Test
    @Throws(ReflectiveOperationException::class)
    fun testKafkaLoggerPrefix() {
        val constField = KafkaAppender::class.java.getDeclaredField("KAFKA_LOGGER_PREFIX")
        if (!constField.isAccessible) {
            constField.isAccessible = true
        }
        val constValue = constField[null] as String
        Assert.assertThat(constValue, Matchers.equalTo("org.apache.kafka.clients"))
    }

    @After
    fun validate() {
        validateMockitoUsage()
    }

}
