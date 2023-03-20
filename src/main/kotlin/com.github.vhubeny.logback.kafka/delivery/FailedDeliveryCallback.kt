package com.github.vhubeny.logback.kafka.delivery

/**
 * @since 0.0.1
 */
interface FailedDeliveryCallback<E> {
    fun onFailedDelivery(evt: E, throwable: Throwable?)
}
