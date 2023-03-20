package com.github.vhubeny.logback.kafka.delivery

import org.apache.kafka.clients.producer.Callback
import org.apache.kafka.clients.producer.Producer
import org.apache.kafka.clients.producer.ProducerRecord
import org.apache.kafka.clients.producer.RecordMetadata
import org.apache.kafka.common.TopicPartition
import org.apache.kafka.common.errors.TimeoutException
import org.junit.Test
import org.mockito.ArgumentCaptor
import org.mockito.ArgumentMatchers
import org.mockito.Mockito
import java.io.IOException

class AsynchronousDeliveryStrategyTest {
    private val producer = Mockito.mock(
        Producer::class.java as Class<Producer<String?, String>>
    )
    private val failedDeliveryCallback = Mockito.mock(
        FailedDeliveryCallback::class.java as Class<FailedDeliveryCallback<String>>
    )
    private val unit = AsynchronousDeliveryStrategy()
    private val topicAndPartition = TopicPartition("topic", 0)
    private val recordMetadata = RecordMetadata(
        topicAndPartition, 0, 0, System
            .currentTimeMillis(), null, 32, 64
    )

    @Test
    fun testCallbackWillNotTriggerOnFailedDeliveryOnNoException() {
        val record = ProducerRecord<String?, String>("topic", 0, null, "msg")
        unit.send(producer, record, "msg", failedDeliveryCallback)
        val callbackCaptor = ArgumentCaptor.forClass(
            Callback::class.java
        )
        Mockito.verify(producer).send(Mockito.refEq(record), callbackCaptor.capture())
        val callback = callbackCaptor.value
        callback.onCompletion(recordMetadata, null)
        Mockito.verify(failedDeliveryCallback, Mockito.never()).onFailedDelivery(
            ArgumentMatchers.anyString(), ArgumentMatchers.any(
                Throwable::class.java
            )
        )
    }

    @Test
    fun testCallbackWillTriggerOnFailedDeliveryOnException() {
        val exception = IOException("KABOOM")
        val record = ProducerRecord<String?, String>("topic", 0, null, "msg")
        unit.send(producer, record, "msg", failedDeliveryCallback)
        val callbackCaptor = ArgumentCaptor.forClass(
            Callback::class.java
        )
        Mockito.verify(producer).send(Mockito.refEq(record), callbackCaptor.capture())
        val callback = callbackCaptor.value
        callback.onCompletion(recordMetadata, exception)
        Mockito.verify(failedDeliveryCallback).onFailedDelivery("msg", exception)
    }

    @Test
    fun testCallbackWillTriggerOnFailedDeliveryOnProducerSendTimeout() {
        val exception = TimeoutException("miau")
        val record = ProducerRecord<String?, String>("topic", 0, null, "msg")
        Mockito.`when`(
            producer.send(
                ArgumentMatchers.same(record), ArgumentMatchers.any(
                    Callback::class.java
                )
            )
        ).thenThrow(exception)
        unit.send(producer, record, "msg", failedDeliveryCallback)
        Mockito.verify(failedDeliveryCallback)
            .onFailedDelivery(ArgumentMatchers.eq("msg"), ArgumentMatchers.same(exception))
    }
}
