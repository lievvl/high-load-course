package ru.quipy.payments.Service

import java.util.*

class PaymentRequest(
    val paymentId: UUID,
    val transactionId: UUID,
    val amount: Int,
    val paymentStartedAt: Long
) {
    public var enqueuedAt: Long = -1;
}

class PaymentRequestComparator {
    companion object : Comparator<PaymentRequest> {
        override fun compare(l: PaymentRequest, r: PaymentRequest): Int = when {
            l.paymentStartedAt != r.paymentStartedAt -> (r.paymentStartedAt - l.paymentStartedAt).toInt()
            else -> 0
        }
    }
}