package com.github.vhubeny.logback.kafka

import ch.qos.logback.classic.spi.ILoggingEvent
import ch.qos.logback.core.Appender
import ch.qos.logback.core.spi.AppenderAttachableImpl
import com.github.vhubeny.logback.kafka.delivery.FailedDeliveryCallback
import org.apache.kafka.clients.producer.KafkaProducer
import org.apache.kafka.clients.producer.Producer
import org.apache.kafka.clients.producer.ProducerConfig
import org.apache.kafka.clients.producer.ProducerRecord
import org.apache.kafka.common.KafkaException
import org.apache.kafka.common.serialization.ByteArraySerializer
import java.util.*
import java.util.concurrent.ConcurrentLinkedQueue

class KafkaAppender<E> : KafkaAppenderConfig<E>() {
    private var producer: LazyProducer? = null
    private val appender = AppenderAttachableImpl<E>()
    private val queue = ConcurrentLinkedQueue<E>()

    private val failedDeliveryCallback = object : FailedDeliveryCallback<E> {
        override fun onFailedDelivery(evt: E?, throwable: Throwable?) {
            appender.appendLoopOnAppenders(evt)
        }
    }

    override fun doAppend(e: E) {
        ensureDeferredAppends()
        if (e is ILoggingEvent && (e as ILoggingEvent).loggerName.toString().startsWith(KAFKA_LOGGER_PREFIX)) {
            deferAppend(e)
        } else {
            super.doAppend(e)
        }
    }

    override fun start() {
        // only error free appenders should be activated
        if (!checkPrerequisites()) return
        if (partition != null && partition!! < 0) {
            partition = null
        }
        producer = LazyProducer()
        super.start()
    }

    override fun stop() {
        super.stop()
        if (producer != null && producer!!.isInitialized) {
            try {
                producer!!.get()!!.close()
            } catch (e: KafkaException) {
                this.addWarn("Failed to shut down kafka producer: " + e.message, e)
            }
            producer = null
        }
    }

    override fun addAppender(newAppender: Appender<E>) {
        appender.addAppender(newAppender)
    }

    override fun iteratorForAppenders(): Iterator<Appender<E>> {
        return appender.iteratorForAppenders()
    }

    override fun getAppender(name: String): Appender<E> {
        return appender.getAppender(name)
    }

    override fun isAttached(appender: Appender<E>): Boolean {
        return this.appender.isAttached(appender)
    }

    override fun detachAndStopAllAppenders() {
        appender.detachAndStopAllAppenders()
    }

    override fun detachAppender(appender: Appender<E>): Boolean {
        return this.appender.detachAppender(appender)
    }

    override fun detachAppender(name: String): Boolean {
        return appender.detachAppender(name)
    }

    public override fun append(e: E) {
        val payload = encoder?.encode(e)
        val key = keyingStrategy?.createKey(e)
        val timestamp = if (isAppendTimestamp) getTimestamp(e) else null
        val record = ProducerRecord<ByteArray, ByteArray>(topic, partition, timestamp, key, payload)
        val producer = producer!!.get()
        if (producer != null) {
            deliveryStrategy?.send(producer, record, e, failedDeliveryCallback)
        } else {
            failedDeliveryCallback.onFailedDelivery(e, null)
        }
    }

    protected fun getTimestamp(e: E): Long {
        return if (e is ILoggingEvent) {
            (e as ILoggingEvent).timeStamp
        } else {
            System.currentTimeMillis()
        }
    }

    protected fun createProducer(): Producer<ByteArray, ByteArray> {
        return KafkaProducer(HashMap(producerConfig))
    }

    private fun deferAppend(event: E) {
        queue.add(event)
    }

    // drains queue events to super
    private fun ensureDeferredAppends() {
        var event: E
        while (queue.poll().also { event = it } != null) {
            super.doAppend(event)
        }
    }

    /**
     * Lazy initializer for producer, patterned after commons-lang.
     *
     * @see [LazyInitializer](https://commons.apache.org/proper/commons-lang/javadocs/api-3.4/org/apache/commons/lang3/concurrent/LazyInitializer.html)
     */
    private inner class LazyProducer {
        @kotlin.jvm.Volatile
        private var producer: Producer<ByteArray, ByteArray>? = null
        fun get(): Producer<ByteArray, ByteArray>? {
            var result = producer
            if (result == null) {
                synchronized(this) {
                    result = producer
                    if (result == null) {
                        result = initialize()
                        producer = result
                    }
                }
            }
            return result
        }

        protected fun initialize(): Producer<ByteArray, ByteArray>? {
            var producer: Producer<ByteArray, ByteArray>? = null
            try {
                producer = createProducer()
            } catch (e: Exception) {
                addError("error creating producer", e)
            }
            return producer
        }

        val isInitialized: Boolean
            get() = producer != null
    }

    companion object {
        /**
         * Kafka clients uses this prefix for its slf4j logging.
         * This appender defers appends of any Kafka logs since it could cause harmful infinite recursion/self feeding effects.
         */
        private val KAFKA_LOGGER_PREFIX: String = KafkaProducer::class.java.getPackage().getName().replaceFirst("\\.producer$".toRegex(), "")
    }

    init {
        // setting these as config values sidesteps an unnecessary warning (minor bug in KafkaProducer)
        addProducerConfigValue(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, ByteArraySerializer::class.qualifiedName!!)
        addProducerConfigValue(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, ByteArraySerializer::class.qualifiedName!!)
    }
}
