package com.github.vhubeny.logback.kafka.util

import java.io.File
import java.io.FileNotFoundException
import java.io.IOException
import java.net.ServerSocket
import java.util.*

internal object TestUtils {
    private val RANDOM = Random()
    fun constructTempDir(dirPrefix: String): File {
        val file = File(System.getProperty("java.io.tmpdir"), dirPrefix + RANDOM.nextInt(10000000))
        if (!file.mkdirs()) {
            throw RuntimeException("could not create temp directory: " + file.absolutePath)
        }
        file.deleteOnExit()
        return file
    }

    val availablePort: Int
        get() = try {
            val socket = ServerSocket(0)
            try {
                socket.localPort
            } finally {
                socket.close()
            }
        } catch (e: IOException) {
            throw IllegalStateException("Cannot find available port: " + e.message, e)
        }

    @Throws(FileNotFoundException::class)
    fun deleteFile(path: File?): Boolean {
        if (!path!!.exists()) {
            throw FileNotFoundException(path.absolutePath)
        }
        var ret = true
        if (path.isDirectory) {
            for (f in path.listFiles()) {
                ret = ret && deleteFile(f)
            }
        }
        return ret && path.delete()
    }
}
