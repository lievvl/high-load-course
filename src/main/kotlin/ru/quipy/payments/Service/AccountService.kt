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

    public val incomingQueue = LinkedBlockingQueue<PaymentRequest>(3000)
    public val incomingExecutor = Executors.newFixedThreadPool(2)

    val rateLimiter = RateLimiter.of(
        "incoming-ratelimiter",
        RateLimiterConfig.custom()
            .limitRefreshPeriod(Duration.ofSeconds(1))
            .limitForPeriod(getLimit())
            .timeoutDuration(PaymentExternalServiceImpl.paymentOperationTimeout)
            .build()
    )

    public fun enqueueAndGetAccount(paymentRequest: PaymentRequest): Account? {
        var accountInHalfOpen: Account? = null
        for (account in accounts) {
            if (!account.circuitBreaker.canProceed()){
                accountInHalfOpen = account
            }
            if(account.tryEnqueue(paymentRequest)) {
                return account
            }
        }
        if (accountInHalfOpen != null) {
            while(!accountInHalfOpen.circuitBreaker.canProceed()) {
            }
            return enqueueAndGetAccount(paymentRequest)
        }
        return null;
    }

    fun resetRateLimit() {
        val limit = getLimit()
        logger.warn("Now ratelimit is ${limit}")
        rateLimiter.changeLimitForPeriod(limit)
    }

    private fun getStartLimit() : Int {
        var summary = 0.0;
        for (account in accounts) {
            summary += min(account.accountConfig.parallelRequests / account.accountConfig.request95thPercentileProcessingTime.seconds.toDouble(), account.accountConfig.rateLimitPerSec.toDouble())
        }
        return summary.toInt()
    }

    private fun getLimit() : Int {
        var summary = 0.0;
        for (account in accounts) {
            summary += account.getRpsDouble()
        }
        return summary.toInt()
    }
}