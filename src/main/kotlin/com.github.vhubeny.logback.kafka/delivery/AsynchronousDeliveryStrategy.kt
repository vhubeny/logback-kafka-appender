package com.github.vhubeny.logback.kafka.delivery

import org.apache.kafka.clients.producer.BufferExhaustedException
import org.apache.kafka.clients.producer.Producer
import org.apache.kafka.clients.producer.ProducerRecord
import org.apache.kafka.common.errors.TimeoutException

class AsynchronousDeliveryStrategy : DeliveryStrategy {
    override fun <K, V, E> send(
        producer: Producer<K, V>, record: ProducerRecord<K, V>, event: E,
        failedDeliveryCallback: FailedDeliveryCallback<E>
    ): Boolean {
        return try {
            producer.send(record) { _, exception ->
                if (exception != null) {
                    failedDeliveryCallback.onFailedDelivery(event, exception)
                }
            }
            true
        } catch (e: BufferExhaustedException) {
            failedDeliveryCallback.onFailedDelivery(event, e)
            false
        } catch (e: TimeoutException) {
            failedDeliveryCallback.onFailedDelivery(event, e)
            false
        }
    }

}
