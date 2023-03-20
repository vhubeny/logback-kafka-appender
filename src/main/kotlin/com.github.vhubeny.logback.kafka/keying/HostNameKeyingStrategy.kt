package com.github.vhubeny.logback.kafka.keying

import ch.qos.logback.core.Context
import ch.qos.logback.core.CoreConstants
import ch.qos.logback.core.spi.ContextAwareBase
import ch.qos.logback.core.spi.LifeCycle
import java.nio.ByteBuffer

/**
 * This strategy uses the HOSTNAME as kafka message key.
 * This is useful because it ensures that all log messages issued by this host will remain in the correct order for any consumer.
 * But this strategy can lead to uneven log distribution for a small number of hosts (compared to the number of partitions).
 */
class HostNameKeyingStrategy : ContextAwareBase(), KeyingStrategy<Any>, LifeCycle {
    private var hostnameHash: ByteArray? = null
    private var errorWasShown = false
    override fun setContext(context: Context) {
        super.setContext(context)
        val hostname = context.getProperty(CoreConstants.HOSTNAME_KEY)
        if (hostname == null) {
            if (!errorWasShown) {
                addError("Hostname could not be found in context. HostNamePartitioningStrategy will not work.")
                errorWasShown = true
            }
        } else {
            hostnameHash = ByteBuffer.allocate(4).putInt(hostname.hashCode()).array()
        }
    }

    override fun createKey(e: Any): ByteArray {
        return hostnameHash!!
    }

    override fun start() {}
    override fun stop() {
        errorWasShown = false
    }

    override fun isStarted(): Boolean {
        return true
    }
}
