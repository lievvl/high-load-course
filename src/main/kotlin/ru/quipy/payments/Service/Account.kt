package ru.quipy.payments.Service

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
import kotlin.time.Duration.Companion.seconds

class Account(
    val accountConfig: ExternalServiceProperties
) {
    val logger = LoggerFactory.getLogger(Account::class.java)
    val processExecutor = Executors.newFixedThreadPool(accountConfig.parallelRequests, NamedThreadFactory("process-${accountConfig.accountName}"))
    val httpExecutor = Executors.newFixedThreadPool(128, NamedThreadFactory("process-${accountConfig.accountName}"))
    val callbackExecutor = Executors.newFixedThreadPool(accountConfig.parallelRequests, NamedThreadFactory("process-${accountConfig.accountName}"))
    val httpClient = OkHttpClient.Builder()
        .dispatcher(Dispatcher(httpExecutor).apply { maxRequests = accountConfig.parallelRequests; maxRequestsPerHost = accountConfig.parallelRequests })
        .protocols(Collections.singletonList(Protocol.H2_PRIOR_KNOWLEDGE))
        .connectionPool(ConnectionPool(accountConfig.parallelRequests, PaymentExternalServiceImpl.paymentOperationTimeout.seconds, TimeUnit.SECONDS))
        .build()

    val queue = PriorityBlockingQueue<PaymentRequest>(accountConfig.parallelRequests, PaymentRequestComparator)

    val timeStatistics = DoubleSummary(accountConfig.request95thPercentileProcessingTime.seconds.toDouble(), 10) //В СЕКУНДАХ!!!!!

    fun tryEnqueue(paymentRequest: PaymentRequest): Boolean {
        if (((timeStatistics.getAverage() * queue.size  + (now() - paymentRequest.paymentStartedAt) / 1000) < PaymentExternalServiceImpl.paymentOperationTimeout.seconds) &&
            queue.remainingCapacity() > 0)
        {
            queue.add(paymentRequest);
            paymentRequest.enqueuedAt = now()
            return true;
        }
        if ((timeStatistics.getAverage() * queue.size  + (now() - paymentRequest.paymentStartedAt) / 1000)
            < PaymentExternalServiceImpl.paymentOperationTimeout.seconds)
        {
            logger.warn("Cannot enqueue because of time ${accountConfig.accountName}")
        } else {
            logger.warn("Cannot enqueue because of size ${accountConfig.accountName}")
        }

        return false;
    }

    private fun now() = System.currentTimeMillis();
}