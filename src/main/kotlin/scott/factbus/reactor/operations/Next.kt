package scott.factbus.reactor.operations

import org.reactivestreams.Publisher
import org.reactivestreams.Subscriber
import org.reactivestreams.Subscription
import java.util.concurrent.atomic.AtomicBoolean

/**
 * Provides a NextPublisher (like a mono) view on an underlying Publisher
 */
class NextPublisher<T>(val parentPublisher: Publisher<T>) : Publisher<T> {
    override fun subscribe(subscriber: Subscriber<in T>) {
        parentPublisher.subscribe(NextSubscriber(subscriber))
    }

    inner class NextSubscriber(val subscriber: Subscriber<in T>) : Subscriber<T> {
        private lateinit var subscription: Subscription
        private val terminated = AtomicBoolean(false)
        override fun onNext(event: T) {
            if (!terminated.compareAndExchange(false, true)) {
                subscriber.onNext(event)
                subscriber.onComplete()  //we only publish the next value, so we tell the subscriber that we are complete
                subscription.cancel() //remove the subscriber from the underlying publisher because he received his single value
            }
        }

        override fun onSubscribe(subscription: Subscription) {
            this.subscription = subscription
            subscriber.onSubscribe(subscription)
        }

        override fun onError(t: Throwable) {
            if (!terminated.compareAndExchange(false, true)) {
                subscriber.onError(t)
            }
        }

        override fun onComplete() {
            if (!terminated.compareAndExchange(false, true)) {
                subscriber.onComplete()
            }
        }
    }
}

fun <T> Publisher<T>.next() = NextPublisher(this)
