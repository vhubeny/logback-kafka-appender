package com.github.vhubeny.logback.kafka

import ch.qos.logback.core.UnsynchronizedAppenderBase
import ch.qos.logback.core.encoder.Encoder
import ch.qos.logback.core.spi.AppenderAttachable
import com.github.vhubeny.logback.kafka.delivery.AsynchronousDeliveryStrategy
import com.github.vhubeny.logback.kafka.delivery.DeliveryStrategy
import com.github.vhubeny.logback.kafka.keying.KeyingStrategy
import com.github.vhubeny.logback.kafka.keying.NoKeyKeyingStrategy
import org.apache.kafka.clients.producer.ProducerConfig.*
import java.util.*


abstract class KafkaAppenderConfig<E> : UnsynchronizedAppenderBase<E>(), AppenderAttachable<E> {

    var topic: String? = null
    var encoder: Encoder<E>? = null
    var keyingStrategy: KeyingStrategy<in E>? = null
    var deliveryStrategy: DeliveryStrategy? = null
    var partition: Int? = null
    var isAppendTimestamp = true
    var producerConfig: MutableMap<String, Any?> = HashMap()
    protected fun checkPrerequisites(): Boolean {
        var errorFree = true
        if (producerConfig[BOOTSTRAP_SERVERS_CONFIG] == null) {
            addError("No \"" + BOOTSTRAP_SERVERS_CONFIG + "\" set for the appender named [\""
                + name + "\"].")
            errorFree = false
        }
        if (topic == null) {
            addError("No topic set for the appender named [\"$name\"].")
            errorFree = false
        }
        if (encoder == null) {
            addError("No encoder set for the appender named [\"$name\"].")
            errorFree = false
        }
        if (keyingStrategy == null) {
            addInfo("No explicit keyingStrategy set for the appender named [\"$name\"]. Using default NoKeyKeyingStrategy.")
            keyingStrategy = NoKeyKeyingStrategy()
        }
        if (deliveryStrategy == null) {
            addInfo("No explicit deliveryStrategy set for the appender named [\"$name\"]. Using default asynchronous strategy.")
            deliveryStrategy = AsynchronousDeliveryStrategy()
        }
        return errorFree
    }

    /*
    fun setEncoder(encoder: Encoder<E>?) {
        this.encoder = encoder
    }

    fun setTopic(topic: String?) {
        this.topic = topic
    }

    fun setKeyingStrategy(keyingStrategy: KeyingStrategy<in E>?) {
        this.keyingStrategy = keyingStrategy
    }
    */

    fun addProducerConfig(keyValue: String) {
        val split: Array<String> = keyValue.split("=").toTypedArray()
        if (split.size == 2) addProducerConfigValue(split[0], split[1])
    }

    fun addProducerConfigValue(key: String, value: Any) {
        producerConfig[key] = value
    }
    /*
    fun getProducerConfig(): Map<String, Any?> {
        return producerConfig
    }

    fun setDeliveryStrategy(deliveryStrategy: DeliveryStrategy?) {
        this.deliveryStrategy = deliveryStrategy
    }

    fun setPartition(partition: Int?) {
        this.partition = partition
    }
    */

}
