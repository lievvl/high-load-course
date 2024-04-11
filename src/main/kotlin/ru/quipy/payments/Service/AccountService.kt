package ru.quipy.payments.Service

import org.springframework.stereotype.Service
import ru.quipy.payments.config.ExternalServicesConfig

@Service
class AccountService {
    private val accounts = ExternalServicesConfig.usedAccounts.map { Account(it) }

    public fun enqueueAndGetAccount(paymentRequest: PaymentRequest): Pair<Account, Long>? {
        for (account in accounts) {
            var time = account.tryEnqueue(paymentRequest)
            if (time != null) {
                return Pair(account, time);
            }
        }
        return null;
    }
}