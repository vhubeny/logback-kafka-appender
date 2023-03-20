package com.github.vhubeny.logback.kafka.keying

/**
 * Evenly distributes all written log messages over all available kafka partitions.
 * This strategy can lead to unexpected read orders on clients.
 */
class NoKeyKeyingStrategy : KeyingStrategy<Any?> {
    override fun createKey(e: Any?): ByteArray? {
        return null
    }
}
