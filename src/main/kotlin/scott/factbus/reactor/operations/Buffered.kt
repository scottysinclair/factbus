package scott.factbus.reactor.operations

import org.reactivestreams.Publisher
import org.reactivestreams.Subscriber
import org.reactivestreams.Subscription
import scott.factbus.reactor.core.roundDownToMaxInt
import java.lang.Math.min
import java.util.concurrent.atomic.AtomicBoolean
import java.util.concurrent.atomic.AtomicInteger
import java.util.concurrent.atomic.AtomicLong

/**
 * Provides a FilteredPublisher view on the underlying  Publisher
 */
class BufferedPublisher<T>(val parentPublisher: Publisher<T>) : Publisher<T> {
    val bufferedSubscriber = BufferedSubscriber<T>()
    init {
        /*
         * subscribe to the parent publisher immediately so that we start to collect and buffer events
         */
        parentPublisher.subscribe(bufferedSubscriber)
    }

    override fun subscribe(subscriber: Subscriber<in T>) {
        bufferedSubscriber.add(subscriber)
    }
}

/**
 * Provides a BufferedSubscriber which subsribes to the underlying publisher and starts to buffer data
 */
class BufferedSubscriber<T> : Subscriber<T> {
    lateinit var realSubscription: Subscription
    val realPublisherComplete = AtomicBoolean(false)
    val subscriptions = mutableListOf<BufferedSubscription<T>>()
    val buffer = mutableListOf<T>()

    fun add(subscriber: Subscriber<in T>) {
        BufferedSubscription(this, subscriber).let {
            subscriptions.add(it)
            subscriber.onSubscribe(it)
        }
    }

    /**
     * Called when the underlying publisher provides data for us
     */
    override fun onNext(event: T) {
        buffer.add(event)
        subscriptions.forEach { it.drain() } //give each subscription a chance to emit from the buffer to it's subscriber
    }

    /**
     * Called when we are subscribed to the underlying publisher
     */
    override fun onSubscribe(subscription: Subscription) {
        this.realSubscription = subscription
        realSubscription.request(Long.MAX_VALUE)
    }

    override fun onError(t: Throwable) = subscriptions.forEach { it.subscriber.onError(t) }

    /**
     * Called when the underlying publisher completes, NOTE: it doesn't mean that WE should forward this... we need to make sure the buffer is drained
     */
    override fun onComplete() {
        realPublisherComplete.set(true)
        subscriptions.forEach { if (it.fullyDrained()) it.subscriber.onComplete() }
    }

    /**
     * Called to remove a buffered subscription
     */
    fun remove(bufferedSubscription: BufferedSubscription<T>) {
        subscriptions.remove(bufferedSubscription)
    }
}

/**
 * Subscription between "our buffer" a given Subscribers which receives the data
 */
class BufferedSubscription<T>(val bufferedSubscriber: BufferedSubscriber<T>, val subscriber : Subscriber<in T>) : Subscription{
    private var subscribersCapacity = AtomicLong(0)
    val bufferConsumed = AtomicInteger(0)

    fun fullyDrained() = bufferConsumed.get() == bufferedSubscriber.buffer.size

    /**
     * drain the next part of the buffer to the subscriber if it has capacity
     */
    fun drain() {
        val to = min(bufferedSubscriber.buffer.size, subscribersCapacity.get().roundDownToMaxInt())
        bufferedSubscriber.buffer.subList(bufferConsumed.get(), to).forEach {
            subscriber.onNext(it)
        }
        bufferConsumed.set(to)
    }

    /**
     * subscriber requests data, if we have data already buffered we can send it straight away
     */
    override fun request(numberOfEventsRequested: Long) {
        subscribersCapacity.updateAndGet { c ->
            if (numberOfEventsRequested > Long.MAX_VALUE - c) Long.MAX_VALUE
            else c + numberOfEventsRequested
        }
        if (!bufferedSubscriber.realPublisherComplete.get()) {
            //TODO: we are not handling capactity here properly
            bufferedSubscriber.realSubscription.request(numberOfEventsRequested)
        }
        drain()
        if (bufferedSubscriber.realPublisherComplete.get() && fullyDrained()) subscriber.onComplete()
    }

    /**
     * Called to cancel the subscription
     */
    override fun cancel() {
        bufferedSubscriber.remove(this)
    }
}


fun <T> Publisher<T>.buffer() = BufferedPublisher(this)
