package com.github.vhubeny.logback.kafka

import ch.qos.logback.classic.Level
import ch.qos.logback.classic.LoggerContext
import ch.qos.logback.classic.spi.ILoggingEvent
import ch.qos.logback.classic.spi.LoggingEvent
import ch.qos.logback.core.BasicStatusManager
import ch.qos.logback.core.encoder.Encoder
import ch.qos.logback.core.status.ErrorStatus
import com.github.vhubeny.logback.kafka.delivery.DeliveryStrategy
import com.github.vhubeny.logback.kafka.delivery.FailedDeliveryCallback
import com.github.vhubeny.logback.kafka.keying.KeyingStrategy
import org.apache.kafka.clients.producer.KafkaProducer
import org.apache.kafka.clients.producer.ProducerRecord
import org.hamcrest.Matchers
import org.junit.After
import org.junit.Assert
import org.junit.Before
import org.junit.Test
import org.mockito.ArgumentCaptor
import org.mockito.ArgumentMatchers
import org.mockito.Mockito

class KafkaAppenderTest {
    val unit = KafkaAppender<ILoggingEvent>()
    val ctx = LoggerContext()
    val encoder: Encoder<ILoggingEvent> = Mockito.mock(
        Encoder::class.java
    ) as Encoder<ILoggingEvent>
    private val keyingStrategy: KeyingStrategy<ILoggingEvent> = Mockito.mock(
        KeyingStrategy::class.java
    ) as KeyingStrategy<ILoggingEvent>
    private val deliveryStrategy = Mockito.mock(
        DeliveryStrategy::class.java
    )

    @Before
    fun before() {
        ctx.name = "testctx"
        ctx.statusManager = BasicStatusManager()
        unit.context = ctx
        unit.name = "kafkaAppenderBase"
        unit.encoder = encoder
        unit.topic = "topic"
        unit.addProducerConfig("bootstrap.servers=localhost:1234")
        unit.keyingStrategy = keyingStrategy
        unit.deliveryStrategy = deliveryStrategy
        ctx.start()
    }

    @After
    fun after() {
        ctx.stop()
        unit.stop()
    }

    @Test
    fun testPerfectStartAndStop() {
        unit.start()
        Assert.assertTrue("isStarted", unit.isStarted)
        unit.stop()
        Assert.assertFalse("isStopped", unit.isStarted)
        Assert.assertThat(ctx.statusManager.copyOfStatusList, Matchers.empty())
        Mockito.verifyZeroInteractions(encoder, keyingStrategy, deliveryStrategy)
    }

    @Test
    fun testDontStartWithoutTopic() {
        unit.topic = null
        unit.start()
        Assert.assertFalse("isStarted", unit.isStarted)
        Assert.assertThat(
            ctx.statusManager.copyOfStatusList,
            Matchers.hasItem(ErrorStatus("No topic set for the appender named [\"kafkaAppenderBase\"].", null))
        )
    }

    @Test
    fun testDontStartWithoutBootstrapServers() {
        unit.producerConfig.clear()
        unit.start()
        Assert.assertFalse("isStarted", unit.isStarted)
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
        unit.encoder = null
        unit.start()
        Assert.assertFalse("isStarted", unit.isStarted)
        Assert.assertThat(
            ctx.statusManager.copyOfStatusList,
            Matchers.hasItem(ErrorStatus("No encoder set for the appender named [\"kafkaAppenderBase\"].", null))
        )
    }

    @Test
    fun testAppendUsesKeying() {
        Mockito.`when`(
            encoder.encode(
                ArgumentMatchers.any(
                    ILoggingEvent::class.java
                )
            )
        ).thenReturn(byteArrayOf(0x00, 0x00))
        unit.start()
        val evt = LoggingEvent("fqcn", ctx.getLogger("logger"), Level.ALL, "message", null, arrayOfNulls(0))
        unit.append(evt)
        verifyMock(evt)
        Mockito.verify(keyingStrategy).createKey(ArgumentMatchers.same(evt))
        verifyMock(evt)
    }

    @Test
    fun testAppendUsesPreConfiguredPartition() {
        Mockito.`when`(
            encoder.encode(
                ArgumentMatchers.any(
                    ILoggingEvent::class.java
                )
            )
        ).thenReturn(byteArrayOf(0x00, 0x00))
        val producerRecordCaptor = ArgumentCaptor.forClass(
            ProducerRecord::class.java
        ) as ArgumentCaptor<ProducerRecord<Any, Any>>
        unit.partition = 1
        unit.start()
        val evt = LoggingEvent("fqcn", ctx.getLogger("logger"), Level.ALL, "message", null, arrayOfNulls(0))
        unit.append(evt)
        verifyMock(evt, producerRecordCaptor)
        val value = producerRecordCaptor.value
        Assert.assertThat(value.partition(), Matchers.equalTo(1))
    }

    @Test
    fun testDeferredAppend() {
        Mockito.`when`(
            encoder.encode(
                ArgumentMatchers.any(
                    ILoggingEvent::class.java
                )
            )
        ).thenReturn(byteArrayOf(0x00, 0x00))
        unit.start()
        val deferredEvent = LoggingEvent(
            "fqcn",
            ctx.getLogger("org.apache.kafka.clients.logger"),
            Level.ALL,
            "deferred message",
            null,
            arrayOfNulls(0)
        )
        unit.doAppend(deferredEvent)
        verifyMock(deferredEvent)

        val evt = LoggingEvent("fqcn", ctx.getLogger("logger"), Level.ALL, "message", null, arrayOfNulls(0))
        unit.doAppend(evt)
        verifyMock(evt)
        verifyMock(evt)
    }

    private fun verifyMock(evt: LoggingEvent, recordCaptor: ArgumentCaptor<ProducerRecord<Any, Any>>? = null) {
        Mockito.verify(deliveryStrategy).send<Any, Any, LoggingEvent>(
            ArgumentMatchers.any(
                KafkaProducer::class.java
            ) as KafkaProducer<Any, Any>,
            recordCaptor?.capture() ?: ArgumentMatchers.any(
                ProducerRecord::class.java
            ) as ProducerRecord<Any, Any>,
            ArgumentMatchers.eq(evt),
            ArgumentMatchers.any(
                FailedDeliveryCallback::class.java
            ) as FailedDeliveryCallback<LoggingEvent>
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
}
