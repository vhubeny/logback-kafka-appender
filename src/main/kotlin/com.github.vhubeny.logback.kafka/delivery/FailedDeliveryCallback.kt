package com.github.vhubeny.logback.kafka.delivery

interface FailedDeliveryCallback<E> {
    fun onFailedDelivery(evt: E, throwable: Throwable?)
}
