package ru.quipy.payments.Service

import io.github.resilience4j.bulkhead.Bulkhead
import io.github.resilience4j.bulkhead.BulkheadConfig
import io.github.resilience4j.ratelimiter.RateLimiter
import io.github.resilience4j.ratelimiter.RateLimiterConfig
import io.github.resilience4j.ratelimiter.RateLimiterRegistry
import okhttp3.ConnectionPool
import okhttp3.Dispatcher
import okhttp3.OkHttpClient
import okhttp3.Protocol
import org.slf4j.LoggerFactory
import ru.quipy.common.utils.*
import ru.quipy.payments.logic.ExternalServiceProperties
import ru.quipy.payments.logic.PaymentExternalServiceImpl
import java.time.Duration
import java.util.*
import java.util.concurrent.Executors
import java.util.concurrent.LinkedBlockingQueue
import java.util.concurrent.PriorityBlockingQueue
import java.util.concurrent.TimeUnit
import kotlin.math.min
import kotlin.time.Duration.Companion.seconds

class Account(
    val accountConfig: ExternalServiceProperties
) {
    val logger = LoggerFactory.getLogger(Account::class.java)

    val processExecutor = Executors.newFixedThreadPool(accountConfig.parallelRequests, NamedThreadFactory("process-${accountConfig.accountName}"))
    val httpExecutor = Executors.newFixedThreadPool(accountConfig.parallelRequests, NamedThreadFactory("process-${accountConfig.accountName}"))
    val callbackExecutor = Executors.newFixedThreadPool(accountConfig.parallelRequests, NamedThreadFactory("callback-${accountConfig.accountName}"))

    val httpClient = OkHttpClient.Builder()
        .dispatcher(Dispatcher(httpExecutor).apply { maxRequests = 200; maxRequestsPerHost = 500 })
        .protocols(Collections.singletonList(Protocol.H2_PRIOR_KNOWLEDGE))
        .connectionPool(ConnectionPool(100, 5, TimeUnit.MINUTES))
        .build()

    val queue = LinkedBlockingQueue<PaymentRequest>()
    val bulkhead = Bulkhead.of(
        "bulkhead-${accountConfig.accountName}",
        BulkheadConfig.custom()
            .maxConcurrentCalls(accountConfig.parallelRequests)
            .maxWaitDuration(PaymentExternalServiceImpl.paymentOperationTimeout)
            .build()
    )

    val timeStatistics = DoubleSummary(accountConfig.request95thPercentileProcessingTime.seconds.toDouble(), 40) //В СЕКУНДАХ!!!!!

    val rateLimiter = RateLimiter.of("ratelimiter-account-${accountConfig.accountName}",
        RateLimiterConfig.custom()
            .limitRefreshPeriod(Duration.ofSeconds(1))
            .limitForPeriod(getRps())
            .timeoutDuration(PaymentExternalServiceImpl.paymentOperationTimeout)
            .build()
    )

    fun tryEnqueue(paymentRequest: PaymentRequest): Boolean {
        if ((getProcessingTimeIfInserting(paymentRequest) < PaymentExternalServiceImpl.paymentOperationTimeout.seconds))
        {
            queue.add(paymentRequest);
            paymentRequest.enqueuedAt = now()
            return true;
        }
        if (getProcessingTimeIfInserting(paymentRequest) < PaymentExternalServiceImpl.paymentOperationTimeout.seconds)
        {
            logger.warn("Cannot enqueue because of time ${accountConfig.accountName}")
        } else {
            logger.warn("Cannot enqueue because of size ${accountConfig.accountName}")
        }

        return false;
    }

    fun getProcessingTimeIfInserting(paymentRequest: PaymentRequest) : Double {
        return timeStatistics.getAverage() * (queue.size + 1)  + (now() - paymentRequest.paymentStartedAt) / 1000
    }

    fun resetRateLimiter() {
        val limit = getRps()
        logger.warn("Now ratelimit is ${limit}")
        rateLimiter.changeLimitForPeriod(limit)
    }

    fun getRps() : Int {
        val ans = min(accountConfig.parallelRequests / timeStatistics.getAverage(), accountConfig.rateLimitPerSec.toDouble()).toInt()
        logger.warn("${accountConfig.accountName} Now rps is ${ans}")
        if (ans <= 0) {
            return 1;
        }
        return ans
    }

    fun getRpsDouble(): Double {
        return min(accountConfig.parallelRequests / timeStatistics.getAverage(), accountConfig.rateLimitPerSec.toDouble());
    }


    private fun now() = System.currentTimeMillis();
}