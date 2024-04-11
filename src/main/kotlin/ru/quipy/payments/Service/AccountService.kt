package ru.quipy.payments.Service

import io.github.resilience4j.ratelimiter.RateLimiter
import io.github.resilience4j.ratelimiter.RateLimiterConfig
import org.slf4j.LoggerFactory
import org.springframework.stereotype.Service
import ru.quipy.payments.config.ExternalServicesConfig
import ru.quipy.payments.logic.PaymentExternalServiceImpl
import java.time.Duration
import java.util.concurrent.Executors
import java.util.concurrent.LinkedBlockingQueue
import java.util.concurrent.PriorityBlockingQueue
import kotlin.math.min

@Service
class AccountService {
    val logger = LoggerFactory.getLogger(AccountService::class.java)
    private val accounts = ExternalServicesConfig.usedAccounts.map { Account(it) }.sortedBy { it.accountConfig.cost }

    public val incomingQueue = PriorityBlockingQueue<PaymentRequest>(3000, PaymentRequestComparator)
    public val incomingExecutor = Executors.newFixedThreadPool(3);
    val rateLimiter = RateLimiter.of("incoming-ratelimiter",
        RateLimiterConfig.custom()
            .limitRefreshPeriod(Duration.ofSeconds(1))
            .limitForPeriod(getLimit())
            .timeoutDuration(PaymentExternalServiceImpl.paymentOperationTimeout)
            .build()
    )

    public fun enqueueAndGetAccount(paymentRequest: PaymentRequest): Account? {
        for (account in accounts) {
            if(account.tryEnqueue(paymentRequest)) {
                return account
            }
        }
        return null;
    }

    fun resetRateLimit() {
        val limit = getLimit()
        logger.warn("Now ratelimit is ${limit}")
        rateLimiter.changeLimitForPeriod(limit)
    }

    private fun getLimit() : Int {
        var summary = 0.0;
        for (account in accounts) {
            summary += min(account.accountConfig.parallelRequests / account.timeStatistics.getAverage(), account.accountConfig.rateLimitPerSec.toDouble())
        }
        return summary.toInt()
    }
}