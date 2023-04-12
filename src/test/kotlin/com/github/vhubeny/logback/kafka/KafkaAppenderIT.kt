package com.github.vhubeny.logback.kafka

import ch.qos.logback.classic.Level
import ch.qos.logback.classic.LoggerContext
import ch.qos.logback.classic.encoder.PatternLayoutEncoder
import ch.qos.logback.classic.spi.ILoggingEvent
import ch.qos.logback.classic.spi.LoggingEvent
import ch.qos.logback.core.Appender
import ch.qos.logback.core.AppenderBase
import ch.qos.logback.core.status.Status
import com.github.vhubeny.logback.kafka.delivery.AsynchronousDeliveryStrategy
import com.github.vhubeny.logback.kafka.keying.NoKeyKeyingStrategy
import com.github.vhubeny.logback.kafka.util.TestKafka
import junit.framework.TestCase
import org.apache.kafka.clients.producer.ProducerConfig
import org.apache.kafka.common.TopicPartition
import org.hamcrest.Matchers
import org.junit.*
import org.junit.rules.ErrorCollector
import java.io.IOException
import java.nio.charset.Charset
import java.util.*
import java.util.concurrent.ThreadLocalRandom

class KafkaAppenderIT {
    @JvmField
    @Rule
    var collector = ErrorCollector()
    private lateinit var kafka: TestKafka
    private lateinit var unit: KafkaAppender<ILoggingEvent>
    private val fallbackLoggingEvents: MutableList<ILoggingEvent> = ArrayList()
    private lateinit var loggerContext: LoggerContext
    @Before
    @Throws(IOException::class, InterruptedException::class)
    fun beforeLogSystemInit() {
        kafka = TestKafka.Companion.createTestKafka(1, 1, 1)
        loggerContext = LoggerContext()
        loggerContext.putProperty("brokers.list", kafka!!.brokerList)
        loggerContext.statusManager.add { status ->
            if (status.effectiveLevel > Status.INFO) {
                System.err.println(status.toString())
                if (status.throwable != null) {
                    collector.addError(status.throwable)
                } else {
                    collector.addError(RuntimeException("StatusManager reported warning: $status"))
                }
            } else {
                println(status.toString())
            }
        }
        loggerContext.putProperty("HOSTNAME", "localhost")
        unit = KafkaAppender()
        val patternLayoutEncoder = PatternLayoutEncoder()
        patternLayoutEncoder.pattern = "%msg"
        patternLayoutEncoder.context = loggerContext
        patternLayoutEncoder.charset = Charset.forName("UTF-8")
        patternLayoutEncoder.start()
        unit.encoder = patternLayoutEncoder
        unit.topic = "logs"
        unit.name = "TestKafkaAppender"
        unit.context = loggerContext
        unit.keyingStrategy = NoKeyKeyingStrategy()
        unit.deliveryStrategy = AsynchronousDeliveryStrategy()
        unit.addAppender(fallbackAppender)
        unit.addProducerConfigValue(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, kafka.brokerList)
        unit.addProducerConfigValue(ProducerConfig.ACKS_CONFIG, "1")
        unit.addProducerConfigValue(ProducerConfig.MAX_BLOCK_MS_CONFIG, "2000")
        unit.addProducerConfigValue(ProducerConfig.LINGER_MS_CONFIG, "100")
        unit.partition = 0
        unit.addAppender(object : AppenderBase<ILoggingEvent>() {
            override fun append(eventObject: ILoggingEvent) {
                fallbackLoggingEvents.add(eventObject)
            }
        })
    }

    private val fallbackAppender: Appender<ILoggingEvent> = object : AppenderBase<ILoggingEvent>() {
        override fun append(eventObject: ILoggingEvent) {
            collector.addError(IllegalStateException("Logged to fallback appender: $eventObject"))
        }
    }

    @After
    fun tearDown() {
        kafka.shutdown()
        kafka.awaitShutdown()
    }

    @Test
    fun testLogging() {
        val messageCount = 2048
        val messageSize = 1024
        val logger = loggerContext.getLogger("ROOT")
        unit.start()
        Assert.assertTrue("appender is started", unit.isStarted)
        val messages = BitSet(messageCount)
        for (i in 0 until messageCount) {
            val prefix = "$i;"
            val sb = StringBuilder()
            sb.append(prefix)
            val b = ByteArray(messageSize - prefix.length)
            ThreadLocalRandom.current().nextBytes(b)
            for (bb : Byte in b) {
                val bbUShort: UShort= (bb.toUShort()).and(127u) //0x7F - aka add 128
                sb.append(bbUShort)
            }
            val loggingEvent = LoggingEvent("a.b.c.d", logger, Level.INFO, sb.toString(), null, arrayOfNulls(0))
            unit.append(loggingEvent)
            messages.set(i)
        }
        unit.stop()
        Assert.assertFalse("appender is stopped", unit.isStarted)
        val javaConsumerConnector = kafka.createClient()
        javaConsumerConnector.assign(listOf(TopicPartition("logs", 0)))
        javaConsumerConnector.seekToBeginning(listOf(TopicPartition("logs", 0)))
        val position = javaConsumerConnector.position(TopicPartition("logs", 0))
        TestCase.assertEquals(0, position)
        var poll = javaConsumerConnector.poll(10000)
        var readMessages = 0
        while (!poll.isEmpty) {
            for (aPoll in poll) {
                val msg = aPoll.value()
                val msgPrefix = ByteArray(32)
                System.arraycopy(msg, 0, msgPrefix, 0, 32)
                val messageFromKafka = String(msgPrefix, UTF8)
                val delimiter = messageFromKafka.indexOf(';')
                val msgNo = messageFromKafka.substring(0, delimiter).toInt()
                messages[msgNo] = false
                readMessages++
            }
            poll = javaConsumerConnector.poll(1000)
        }
        TestCase.assertEquals(messageCount, readMessages)
        Assert.assertThat<List<ILoggingEvent>>(fallbackLoggingEvents, Matchers.empty())
        TestCase.assertEquals("all messages should have been read", BitSet.valueOf(ByteArray(0)), messages)
    }

    companion object {
        private val UTF8 = Charset.forName("UTF-8")
    }
}
