package com.github.vhubeny.logback.kafka

import com.github.vhubeny.logback.kafka.util.TestKafka
import com.github.vhubeny.logback.kafka.util.TestKafka.Companion.createTestKafka
import org.apache.kafka.common.TopicPartition
import org.hamcrest.Matchers
import org.junit.*
import org.junit.rules.ErrorCollector
import org.slf4j.Logger
import org.slf4j.LoggerFactory
import java.io.IOException
import java.nio.charset.Charset

class LogbackIntegrationIT {
    @Rule
    var collector = ErrorCollector()
    private var kafka: TestKafka? = null
    private var logger: Logger? = null
    @Before
    @Throws(IOException::class, InterruptedException::class)
    fun beforeLogSystemInit() {
        kafka = createTestKafka(listOf(9092))
        logger = LoggerFactory.getLogger("LogbackIntegrationIT")
    }

    @After
    fun tearDown() {
        kafka!!.shutdown()
        kafka!!.awaitShutdown()
    }

    @Test
    fun testLogging() {
        for (i in 0..999) {
            logger!!.info("message$i")
        }
        val client = kafka!!.createClient()
        client.assign(listOf(TopicPartition("logs", 0)))
        client.seekToBeginning(listOf(TopicPartition("logs", 0)))
        var no = 0
        var poll = client.poll(1000)
        while (!poll.isEmpty) {
            for (consumerRecord in poll) {
                val messageFromKafka = String(consumerRecord.value(), UTF8)
                Assert.assertThat(
                    messageFromKafka, Matchers.equalTo(
                        "message$no"
                    )
                )
                ++no
            }
            poll = client.poll(1000)
        }
        Assert.assertEquals(1000, no.toLong())
    }

    companion object {
        private val UTF8 = Charset.forName("UTF-8")
    }
}
