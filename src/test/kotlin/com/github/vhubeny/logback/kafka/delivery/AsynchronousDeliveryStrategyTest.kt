package com.github.vhubeny.logback.kafka.delivery

import com.nhaarman.mockitokotlin2.*
import org.apache.kafka.clients.producer.Callback
import org.apache.kafka.clients.producer.Producer
import org.apache.kafka.clients.producer.ProducerRecord
import org.apache.kafka.clients.producer.RecordMetadata
import org.apache.kafka.common.TopicPartition
import org.apache.kafka.common.errors.TimeoutException
import org.junit.Before
import org.junit.Test
import org.mockito.*
import org.mockito.ArgumentMatchers.anyString
import org.mockito.Mockito.`when`
import org.mockito.MockitoAnnotations.openMocks
import java.io.IOException

class AsynchronousDeliveryStrategyTest {
    @Captor
    private lateinit var callbackCaptor: ArgumentCaptor<Callback>
    private val producer: Producer<String?, String> = mock()
    private val failedDeliveryCallback: FailedDeliveryCallback<String> = Mockito.mock()
    private val unit = AsynchronousDeliveryStrategy()
    private val topicAndPartition = TopicPartition("topic", 0)
    private val recordMetadata = RecordMetadata(
        topicAndPartition, 0, 0, System
            .currentTimeMillis(), null, 32, 64
    )

    @Before
    fun before() {
        openMocks(this);
    }
    @Test
    fun testCallbackWillNotTriggerOnFailedDeliveryOnNoException() {
        val record = ProducerRecord<String?, String>("topic", 0, null, "msg")
        unit.send(producer, record, "msg", failedDeliveryCallback)

        verify(producer).send(Mockito.refEq(record), callbackCaptor.capture())
        val callback = callbackCaptor.value
        callback.onCompletion(recordMetadata, null)
        verify(failedDeliveryCallback, never()).onFailedDelivery(
            anyString(), any()
        )
    }

    @Test
    fun testCallbackWillTriggerOnFailedDeliveryOnException() {
        val exception = IOException("KABOOM")
        val record = ProducerRecord<String?, String>("topic", 0, null, "msg")
        unit.send(producer, record, "msg", failedDeliveryCallback)
        verify(producer).send(Mockito.refEq(record), callbackCaptor.capture())
        val callback = callbackCaptor.value
        callback.onCompletion(recordMetadata, exception)
        verify(failedDeliveryCallback).onFailedDelivery(eq("msg"), eq(exception))
    }

    @Test
    fun testCallbackWillTriggerOnFailedDeliveryOnProducerSendTimeout() {
        val exception = TimeoutException("miau")
        val record = ProducerRecord<String?, String>("topic", 0, null, "msg")
        `when`(
            producer.send(same(record), any())
        ).thenThrow(exception)
        unit.send(producer, record, "msg", failedDeliveryCallback)
        verify(failedDeliveryCallback).onFailedDelivery(eq("msg"), same(exception))
    }
}
