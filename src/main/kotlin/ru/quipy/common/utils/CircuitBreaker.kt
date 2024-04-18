package ru.quipy.common.utils

import org.slf4j.LoggerFactory
import java.util.concurrent.ConcurrentLinkedQueue
import java.util.concurrent.Executors
import java.util.concurrent.LinkedBlockingQueue
import java.util.concurrent.TimeUnit
import java.util.concurrent.atomic.AtomicInteger
import java.util.concurrent.atomic.AtomicIntegerArray
import java.util.concurrent.atomic.AtomicReference

class MyCircuitBreaker(
    val name: String,
    private val failureRateThreshold: Double,
    private val slidingWindowSize: Int,
    private val allowedRequestsInHalfOpen: Int,
    private val resetTimeoutMs: Long
) {

    enum class CircuitBreakerState {
        CLOSED, OPEN, HALF_OPEN
    }


    private var successCount = AtomicInteger(0)
    private var failureCount = AtomicInteger(0)
    private var count = AtomicInteger(0)
    private var results = LinkedBlockingQueue<Int>(slidingWindowSize)

    private var lastStateChangeTime = AtomicReference(System.currentTimeMillis())
    private var state = AtomicReference(CircuitBreakerState.CLOSED)
    private val executor = Executors.newSingleThreadScheduledExecutor(NamedThreadFactory(name))
    private val logger = LoggerFactory.getLogger(MyCircuitBreaker::class.java)

    init {
        executor.scheduleAtFixedRate({
            update()
        }, 0, 2_000, TimeUnit.MILLISECONDS)
    }

    fun canProceed() : Boolean {
        return state.get() != CircuitBreakerState.OPEN
    }

    fun canMakeCall() : Boolean {
        if (state.get() == CircuitBreakerState.CLOSED) {
            return true
        }
        if (state.get() == CircuitBreakerState.HALF_OPEN) {
            while(true) {
                val value = count.get()
                if (value + 1 > allowedRequestsInHalfOpen) {
                    return false
                }
                if (value + 1 <= allowedRequestsInHalfOpen && count.compareAndSet(value, value + 1)) {
                    return true
                }
            }
        }
        return false
    }

    fun submitFailure() {
        updateCount(0)
        logger.warn("[$name] cb failure: success is ${successCount.get()}")
        logger.warn("[$name] cb failure: failure is ${failureCount.get()}")
        logger.warn("[$name] cb failure: count is ${count.get()}")
        logger.warn("[$name] cb failure: lastAdded is ${lastAdded.get()}")
        when (state.get()) {
            CircuitBreakerState.CLOSED -> {
                if (failureRate() >= failureRateThreshold) {
                    transitionToOpen()
                }
            }
            CircuitBreakerState.OPEN -> {}
            CircuitBreakerState.HALF_OPEN -> {
                transitionToOpen()
            }
        }
    }

    fun submitSuccess() {
        updateCount(1)
        logger.warn("[$name] cb success: success is ${successCount.get()}")
        logger.warn("[$name] cb success: failure is ${failureCount.get()}")
        logger.warn("[$name] cb success: count is ${count.get()}")
        logger.warn("[$name] cb success: lastAdded is ${lastAdded.get()}")
        when (state.get()) {
            CircuitBreakerState.CLOSED -> {}
            CircuitBreakerState.OPEN -> {}
            CircuitBreakerState.HALF_OPEN -> {
                if (count.get() == allowedRequestsInHalfOpen) {
                    transitionToClosed()
                }
            }
        }
    }

    private fun updateCount(toAdd : Int) {
        count.incrementAndGet()
        if (count.get() > slidingWindowSize) {
            while(true) {
                val value = lastAdded.get()
                if (value == 1 && lastAdded.compareAndSet(value, toAdd)) {
                    successCount.decrementAndGet()
                    break;
                }
                if (value == 0 && lastAdded.compareAndSet(value, toAdd)) {
                    failureCount.decrementAndGet()
                    break;
                }
            }
        }
        if (toAdd == 1) {
            successCount.incrementAndGet()
        } else {
            failureCount.incrementAndGet()
        }
    }

    private fun failureRate(): Double {
        if (count.get() < slidingWindowSize) {
            return 0.0
        }
        return failureCount.get() / slidingWindowSize.toDouble()
    }

    private fun transitionToOpen() {
        while (!state.compareAndSet(CircuitBreakerState.CLOSED, CircuitBreakerState.OPEN)) {
            if (state.get() == CircuitBreakerState.OPEN) {
                return
            }
            if (state.get() == CircuitBreakerState.HALF_OPEN) {
                state.compareAndSet(CircuitBreakerState.HALF_OPEN, CircuitBreakerState.OPEN)
            }
        }
        count = AtomicInteger(0)
        successCount = AtomicInteger(0)
        failureCount = AtomicInteger(0)

        lastStateChangeTime.set(System.currentTimeMillis())
        onStateChange(state.get())
    }

    private fun transitionToHalfOpen() {
        while (!state.compareAndSet(CircuitBreakerState.OPEN, CircuitBreakerState.HALF_OPEN)) {
            if (state.get() == CircuitBreakerState.HALF_OPEN) {
                return
            }
            if (state.get() == CircuitBreakerState.CLOSED) {
                logger.warn("[$name] breaker is transitioning from CLOSED to HALF OPEN!")
                return
            }
        }
        lastStateChangeTime.set(System.currentTimeMillis())
        onStateChange(state.get())
    }

    private fun transitionToClosed() {
        while (!state.compareAndSet(CircuitBreakerState.HALF_OPEN, CircuitBreakerState.CLOSED)) {
            if (state.get() == CircuitBreakerState.CLOSED) {
                return
            }
            if (state.get() == CircuitBreakerState.OPEN) {
                logger.warn("[$name] breaker is transitioning from OPEN to CLOSED!")
                return
            }
        }
        lastStateChangeTime.set(System.currentTimeMillis())
        onStateChange(state.get())
    }

    private fun onStateChange(state: CircuitBreakerState) {
        logger.error("[$name] now in state $state")
    }

    fun update() {
        if (state.get() == CircuitBreakerState.OPEN && System.currentTimeMillis() - lastStateChangeTime.get() >= resetTimeoutMs) {
            transitionToHalfOpen()
        }
    }

    fun destroy() {
        logger.info("Shutting down the CircuitBreaker executor")
        executor.shutdown()
    }
}