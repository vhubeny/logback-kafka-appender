package com.github.vhubeny.logback.kafka.util

import org.apache.zookeeper.server.NIOServerCnxnFactory
import org.apache.zookeeper.server.ServerCnxnFactory
import org.apache.zookeeper.server.ZooKeeperServer
import org.junit.Assert
import java.io.File
import java.io.FileNotFoundException
import java.io.IOException
import java.net.InetSocketAddress

class EmbeddedZookeeper @JvmOverloads constructor(port: Int = -1, tickTime: Int = 500) {
    private var port = -1
    private var tickTime = 100
    private var factory: ServerCnxnFactory? = null
    private var snapshotDir: File? = null
    private var logDir: File? = null

    init {
        this.port = resolvePort(port)
        this.tickTime = tickTime
    }

    private fun resolvePort(port: Int): Int {
        return if (port == -1) {
            TestUtils.availablePort
        } else port
    }

    @Throws(IOException::class)
    fun startup() {
        if (port == -1) {
            port = TestUtils.availablePort
        }
        factory = NIOServerCnxnFactory.createFactory(InetSocketAddress("localhost", port), 1024)
        snapshotDir = TestUtils.constructTempDir("embeeded-zk/snapshot")
        logDir = TestUtils.constructTempDir("embeeded-zk/log")
        val zooKeeperServer = ZooKeeperServer(snapshotDir, logDir, tickTime)
        try {
            factory!!.startup(zooKeeperServer)
        } catch (e: InterruptedException) {
            throw IOException(e)
        }
        Assert.assertEquals("standalone", zooKeeperServer.state)
        Assert.assertEquals(port.toLong(), zooKeeperServer.clientPort.toLong())
    }

    fun shutdown() {
        factory!!.shutdown()
        try {
            factory!!.join()
        } catch (e: InterruptedException) {
            throw IllegalStateException("should not happen: " + e.message, e)
        }
        try {
            TestUtils.deleteFile(snapshotDir)
        } catch (e: FileNotFoundException) {
            // ignore
        }
        try {
            TestUtils.deleteFile(logDir)
        } catch (e: FileNotFoundException) {
            // ignore
        }
    }

    val connection: String
        get() = "localhost:$port"

    override fun toString(): String {
        return "EmbeddedZookeeper{" + "connection=" + connection + '}'
    }
}
