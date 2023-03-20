package com.github.vhubeny.logback.kafka.util

import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.kafka.clients.consumer.KafkaConsumer
import org.apache.kafka.common.serialization.ByteArrayDeserializer
import java.io.IOException

class TestKafka internal constructor(
    private val zookeeper: EmbeddedZookeeper,
    private val kafkaCluster: EmbeddedKafkaCluster
) {
    @JvmOverloads
    fun createClient(consumerProperties: MutableMap<String?, Any?> = HashMap()): KafkaConsumer<ByteArray, ByteArray> {
        consumerProperties[ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG] = brokerList
        //consumerProperties.put("group.id", "simple-consumer-" + new Random().nextInt());
        consumerProperties[ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG] = "false"
        consumerProperties["auto.offset.reset"] = "earliest"
        consumerProperties["key.deserializer"] = ByteArrayDeserializer::class.java.name
        consumerProperties["value.deserializer"] =
            ByteArrayDeserializer::class.java.name
        return KafkaConsumer(consumerProperties)
    }

    val zookeeperConnection: String?
        get() = zookeeper.connection
    val brokerList: String?
        get() = kafkaCluster.brokerList

    fun shutdown() {
        try {
            kafkaCluster.shutdown()
        } finally {
            zookeeper.shutdown()
        }
    }

    fun awaitShutdown() {
        kafkaCluster.awaitShutdown()
    }

    companion object {
        @Throws(IOException::class, InterruptedException::class)
        fun createTestKafka(brokerCount: Int, partitionCount: Int, replicationFactor: Int): TestKafka {
            val ports: MutableList<Int> = ArrayList(brokerCount)
            for (i in 0 until brokerCount) {
                ports.add(-1)
            }
            val properties: MutableMap<String?, String?> = HashMap()
            properties["num.partitions"] = Integer.toString(partitionCount)
            properties["default.replication.factor"] = Integer.toString(replicationFactor)
            return createTestKafka(ports, properties)
        }

        @JvmOverloads
        @Throws(IOException::class, InterruptedException::class)
        fun createTestKafka(
            brokerPorts: List<Int>,
            properties: Map<String?, String?>? = emptyMap<String?, String>()
        ): TestKafka {
            var properties = properties
            if (properties == null) properties = emptyMap<String?, String>()
            val zk = EmbeddedZookeeper(-1, 100)
            zk.startup()
            val kafka = EmbeddedKafkaCluster(zk.connection, properties, brokerPorts)
            kafka.startup()
            return TestKafka(zk, kafka)
        }
    }
}
