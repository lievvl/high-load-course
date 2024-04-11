package ru.quipy.payments.Service

import java.util.*

class PaymentRequest(
    val paymentId: UUID,
    val transactionId: UUID,
    val paymentStartedAt: Long
)

class PaymentRequestComparator {
    companion object : Comparator<PaymentRequest> {

        override fun compare(l: PaymentRequest, r: PaymentRequest): Int = when {
            l.paymentStartedAt != r.paymentStartedAt -> (r.paymentStartedAt - l.paymentStartedAt).toInt()
            else -> 0
        }
    }
}