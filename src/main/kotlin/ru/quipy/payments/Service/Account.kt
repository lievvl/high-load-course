package ru.quipy.payments.Service

import okhttp3.ConnectionPool
import okhttp3.Dispatcher
import okhttp3.OkHttpClient
import okhttp3.Protocol
import ru.quipy.common.utils.DoubleSummary
import ru.quipy.common.utils.OngoingWindow
import ru.quipy.common.utils.RateLimiter
import ru.quipy.common.utils.Summary
import ru.quipy.payments.logic.ExternalServiceProperties
import ru.quipy.payments.logic.PaymentExternalServiceImpl
import java.util.*
import java.util.concurrent.Executors
import java.util.concurrent.PriorityBlockingQueue
import java.util.concurrent.TimeUnit

class Account(
    private val accountConfig: ExternalServiceProperties
) {
    private val processExecutor = Executors.newFixedThreadPool(accountConfig.parallelRequests)
    private val callbackExecutor = Executors.newFixedThreadPool(accountConfig.parallelRequests)
    private val httpClient = OkHttpClient.Builder()
        .dispatcher(Dispatcher(processExecutor).apply { maxRequests = accountConfig.parallelRequests })
        .protocols(Collections.singletonList(Protocol.H2_PRIOR_KNOWLEDGE))
        .connectionPool(ConnectionPool(accountConfig.parallelRequests, PaymentExternalServiceImpl.paymentOperationTimeout.seconds, TimeUnit.SECONDS))
        .build()

    private val queue = PriorityBlockingQueue<PaymentRequest>(50, PaymentRequestComparator)

    private val processWindow = OngoingWindow(accountConfig.parallelRequests)
    private val rateLimiter = RateLimiter(accountConfig.rateLimitPerSec)
    private val timeStatistics = DoubleSummary(accountConfig.request95thPercentileProcessingTime.toMillis().toDouble(), 10) //В МИЛЛИСЕКУНДАХ!!!!!

    fun tryEnqueue(paymentRequest: PaymentRequest): Long? {
        if (timeStatistics.getAverage() * (queue.size + 1) + now() <
            PaymentExternalServiceImpl.paymentOperationTimeout.toMillis() + paymentRequest.paymentStartedAt
            ) {
            queue.add(paymentRequest);
            return now();
        }
        return null;
    }

    private fun now() = System.currentTimeMillis();
}