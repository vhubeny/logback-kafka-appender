package com.github.vhubeny.logback.kafka.util

import ch.qos.logback.classic.Logger
import com.carrotsearch.junitbenchmarks.BenchmarkOptions
import com.carrotsearch.junitbenchmarks.BenchmarkRule
import com.carrotsearch.junitbenchmarks.annotation.AxisRange
import com.carrotsearch.junitbenchmarks.annotation.BenchmarkHistoryChart
import com.carrotsearch.junitbenchmarks.annotation.BenchmarkMethodChart
import com.carrotsearch.junitbenchmarks.annotation.LabelType
import org.junit.*
import org.junit.rules.TestRule
import org.slf4j.LoggerFactory

@AxisRange(min = 0.0, max = 5.0)
@BenchmarkMethodChart(filePrefix = "benchmark-lists")
@BenchmarkHistoryChart(labelWith = LabelType.CUSTOM_KEY, maxRuns = 20)
class KafkaAppenderBenchmark {
    @Rule
    var benchmarkRun: TestRule = BenchmarkRule()
    private lateinit var logger: Logger
    @Before
    fun before() {
        LoggerFactory.getLogger("triggerLogInitialization")
        logger = LoggerFactory.getLogger("IT") as Logger
    }

    @After
    fun after() {
    }

    @Ignore
    @BenchmarkOptions(benchmarkRounds = 10, warmupRounds = 2, concurrency = 8)
    @Test
    @Throws(
        InterruptedException::class
    )
    fun benchmark() {
        for (i in 0..99999) {
            logger.info("A VERY IMPORTANT LOG MESSAGE {}", i)
        }
    }
}
