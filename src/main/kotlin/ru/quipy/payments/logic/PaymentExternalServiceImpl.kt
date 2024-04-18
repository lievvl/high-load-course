package ru.quipy.payments.logic

import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.module.kotlin.registerKotlinModule
import okhttp3.*
import org.slf4j.LoggerFactory
import org.springframework.beans.factory.annotation.Autowired
import ru.quipy.core.EventSourcingService
import ru.quipy.payments.Service.Account
import ru.quipy.payments.Service.AccountService
import ru.quipy.payments.Service.PaymentRequest
import ru.quipy.payments.api.PaymentAggregate
import java.io.IOException
import java.net.SocketTimeoutException
import java.time.Duration
import java.util.*
import java.util.concurrent.Executors
import java.util.concurrent.ThreadPoolExecutor
import java.util.concurrent.TimeoutException
import java.util.concurrent.atomic.AtomicInteger


// Advice: always treat time as a Duration
class PaymentExternalServiceImpl(
    private val accountService: AccountService,
) : PaymentExternalService {

    companion object {
        val logger = LoggerFactory.getLogger(PaymentExternalServiceImpl::class.java)

        val paymentOperationTimeout = Duration.ofSeconds(80)

        val emptyBody = RequestBody.create(null, ByteArray(0))
        val mapper = ObjectMapper().registerKotlinModule()
        val processedSuccess = AtomicInteger(0);
    }

    @Autowired
    private lateinit var paymentESService: EventSourcingService<UUID, PaymentAggregate, PaymentAggregateState>

//    override fun submitPaymentRequest(paymentId: UUID, amount: Int, paymentStartedAt: Long) {
//        logger.warn("[$accountName] Submitting payment request for payment $paymentId. Already passed: ${now() - paymentStartedAt} ms")
//
//        val transactionId = UUID.randomUUID()
//        logger.info("[$accountName] Submit for $paymentId , txId: $transactionId")
//
//        // Вне зависимости от исхода оплаты важно отметить что она была отправлена.
//        // Это требуется сделать ВО ВСЕХ СЛУЧАЯХ, поскольку эта информация используется сервисом тестирования.
//        paymentESService.update(paymentId) {
//            it.logSubmission(success = true, transactionId, now(), Duration.ofMillis(now() - paymentStartedAt))
//        }
//
//        val request = Request.Builder().run {
//            url("http://localhost:1234/external/process?serviceName=${serviceName}&accountName=${accountName}&transactionId=$transactionId")
//            post(emptyBody)
//        }.build()
//
//        try {
//            client.newCall(request).execute().use { response ->
//                val body = try {
//                    mapper.readValue(response.body?.string(), ExternalSysResponse::class.java)
//                } catch (e: Exception) {
//                    logger.error("[$accountName] [ERROR] Payment processed for txId: $transactionId, payment: $paymentId, result code: ${response.code}, reason: ${response.body?.string()}")
//                    ExternalSysResponse(false, e.message)
//                }
//
//                logger.warn("[$accountName] Payment processed for txId: $transactionId, payment: $paymentId, succeeded: ${body.result}, message: ${body.message}")
//
//                // Здесь мы обновляем состояние оплаты в зависимости от результата в базе данных оплат.
//                // Это требуется сделать ВО ВСЕХ ИСХОДАХ (успешная оплата / неуспешная / ошибочная ситуация)
//                paymentESService.update(paymentId) {
//                    it.logProcessing(body.result, now(), transactionId, reason = body.message)
//                }
//            }
//        } catch (e: Exception) {
//            when (e) {
//                is SocketTimeoutException -> {
//                    paymentESService.update(paymentId) {
//                        it.logProcessing(false, now(), transactionId, reason = "Request timeout.")
//                    }
//                }
//
//                else -> {
//                    logger.error("[$accountName] Payment failed for txId: $transactionId, payment: $paymentId", e)
//
//                    paymentESService.update(paymentId) {
//                        it.logProcessing(false, now(), transactionId, reason = e.message)
//                    }
//                }
//            }
//        }
//    }
    override fun submitPaymentRequest(paymentId: UUID, amount: Int, paymentStartedAt: Long) {
        logger.warn("Starting processing ${paymentId}, ${paymentStartedAt}")
        val request = PaymentRequest(paymentId, UUID.randomUUID(), amount, paymentStartedAt)
        if (!isOnTime(paymentStartedAt)) {
            failedTransaction(request, "Cannot process ${request.paymentId} because its too late!")
            return
        }
        accountService.incomingQueue.add(request)
        accountService.incomingExecutor.submit {
            while(!accountService.rateLimiter.acquirePermission()) {
                continue
            }
            enqueuePaymentRequest()
        }
    }
    private fun enqueuePaymentRequest() {
        val paymentRequest = accountService.incomingQueue.poll()
        val account = accountService.enqueueAndGetAccount(paymentRequest)
        if (account == null) {
            failedTransaction(paymentRequest, "Cannot process ${paymentRequest.paymentId}: Cannot euqueue request")
            return
        }
//        account.queue.add(PaymentRequest(UUID.randomUUID(), UUID.randomUUID(), 50, 1))
//        logger.warn("${account.queue.peek().paymentStartedAt}")

        account.processExecutor.submit {
            account.bulkhead.acquirePermission();
            processPaymentRequest(account)
            account.bulkhead.releasePermission()
        }
    }

    private fun processPaymentRequest(account: Account) {
        val request = account.queue.poll()

        logger.warn("[{${account.accountConfig.accountName}}] Estimated processing time for {${request.paymentId}} is ${account.getProcessingTimeIfInserting(request)}")
        if (account.getProcessingTimeIfInserting(request) > paymentOperationTimeout.seconds) {
            failedTransaction(request, "Cannot process ${request.paymentId}: Enqueued, but failed to be in time")
            return
        }

        while(!account.rateLimiter.acquirePermission()){
        }

        val httpRequest = Request.Builder().run {
            url("http://localhost:1234/external/process?serviceName=${account.accountConfig.serviceName}&accountName=${account.accountConfig.accountName}&transactionId=${request.transactionId}")
            post(emptyBody)
        }.build()

        account.httpClient.newCall(httpRequest).enqueue(object : Callback {
            override fun onFailure(call: Call, e: IOException) {
                account.callbackExecutor.submit {
                    logger.warn("[${account.accountConfig.accountName}]Avg time in queue: ${account.timeStatistics.getAverage()}")
                    account.timeStatistics.reportExecution((now() - request.enqueuedAt).toDouble() / 1000)
                    when (e) {
                        is SocketTimeoutException -> {
                            paymentESService.update(request.paymentId) {
                                it.logProcessing(false, now(), request.transactionId, reason = "Request timeout.")
                            }
                        }

                        else -> {
                            logger.error("[${account.accountConfig.accountName}] Payment failed for txId: ${request.transactionId}, payment: ${request.paymentId}", e)
                            paymentESService.update(request.paymentId) {
                                it.logProcessing(false, now(), request.transactionId, reason = e.message)
                            }
                        }
                    }
                    accountService.resetRateLimit()
                    account.resetRateLimiter()
                }
            }

            override fun onResponse(call: Call, response: Response) {
                account.callbackExecutor.submit {
                    logger.warn("[${account.accountConfig.accountName}]Avg time in queue: ${account.timeStatistics.getAverage()}")
                    account.timeStatistics.reportExecution((now() - request.enqueuedAt).toDouble() / 1000)
                    val body = try {
                        mapper.readValue(response.body?.string(), ExternalSysResponse::class.java)
                    } catch (e: Exception) {
                        ExternalSysResponse(false, e.message)
                    }

                    logger.warn("[${account.accountConfig.accountName}] Payment processed for txId: ${request.transactionId}," +
                            " payment: ${request.paymentId}, succeeded: ${body.result}, message: ${body.message}")
                    paymentESService.update(request.paymentId) {
                        it.logProcessing(body.result, now(), request.transactionId, reason = body.message)
                    }
                    logger.warn("Success now is ${processedSuccess.incrementAndGet()}")
                    accountService.resetRateLimit()
                    account.resetRateLimiter()
                }
            }
        })
    }

    private fun isOnTime(paymentStartedAt: Long): Boolean {
        return now() - paymentStartedAt < paymentOperationTimeout.toMillis()
    }

    private fun failedTransaction(paymentRequest: PaymentRequest, message: String) {
        logger.warn(message)
        paymentESService.update(paymentRequest.paymentId) {
            it.logProcessing(false, now(), paymentRequest.transactionId, reason = message)
        }
    }
}

public fun now() = System.currentTimeMillis()