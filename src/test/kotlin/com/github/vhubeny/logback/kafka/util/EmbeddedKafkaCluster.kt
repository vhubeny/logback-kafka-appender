package com.github.vhubeny.logback.kafka.util

import kafka.server.KafkaConfig
import kafka.server.KafkaServer
import org.apache.kafka.common.utils.Time
import scala.Some
import scala.collection.immutable.`Vector$`
import java.io.File
import java.io.FileNotFoundException

class EmbeddedKafkaCluster(val zkConnection: String?, baseProperties: Map<String?, String?>, ports: List<Int>) {
    val ports: List<Int>
    private val baseProperties: Map<String?, String?>
    val brokerList: String
    private val brokers: MutableList<KafkaServer>
    private val logDirs: MutableList<File?>

    init {
        this.ports = resolvePorts(ports)
        this.baseProperties = baseProperties
        brokers = ArrayList()
        logDirs = ArrayList()
        brokerList = constructBrokerList(this.ports)
    }

    private fun resolvePorts(ports: List<Int>): List<Int> {
        val resolvedPorts: MutableList<Int> = ArrayList()
        for (port in ports) {
            resolvedPorts.add(resolvePort(port))
        }
        return resolvedPorts
    }

    private fun resolvePort(port: Int): Int {
        return if (port == -1) {
            TestUtils.availablePort
        } else port
    }

    private fun constructBrokerList(ports: List<Int>): String {
        val sb = StringBuilder()
        for (port in ports) {
            if (sb.length > 0) {
                sb.append(",")
            }
            sb.append("localhost:").append(port)
        }
        return sb.toString()
    }

    fun startup() {
        for (i in ports.indices) {
            val port = ports[i]
            val logDir = TestUtils.constructTempDir("kafka-local")
            val properties: MutableMap<String?, String?> = HashMap()
            properties.putAll(baseProperties)
            properties["zookeeper.connect"] = zkConnection
            properties["broker.id"] = (i + 1).toString()
            properties["host.name"] = "localhost"
            properties["port"] = Integer.toString(port)
            properties["log.dir"] = logDir!!.absolutePath
            properties["log.flush.interval.messages"] = 1.toString()
            properties["advertised.host.name"] = "localhost"
            val broker = startBroker(properties)
            brokers.add(broker)
            logDirs.add(logDir)
        }
    }

    private fun startBroker(props: Map<String?, String?>): KafkaServer {
        val server = KafkaServer(
            KafkaConfig(props), Time.SYSTEM,
            Some.apply("embedded-kafka-cluster"), `Vector$`.`MODULE$`.empty()
        )
        server.startup()
        return server
    }

    fun shutdown() {
        for (broker in brokers) {
            try {
                broker.shutdown()
                broker.awaitShutdown()
            } catch (e: Exception) {
                e.printStackTrace()
            }
        }
        for (logDir in logDirs) {
            try {
                TestUtils.deleteFile(logDir)
            } catch (e: FileNotFoundException) {
                e.printStackTrace()
            }
        }
    }

    fun awaitShutdown() {
        for (broker in brokers) {
            broker.awaitShutdown()
        }
    }

    override fun toString(): String {
        val sb = StringBuilder("EmbeddedKafkaCluster{")
        sb.append("boostrapServers='").append(brokerList).append('\'')
        sb.append('}')
        return sb.toString()
    }
}
