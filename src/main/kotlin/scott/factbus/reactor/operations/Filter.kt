package scott.factbus.reactor.operations

import org.reactivestreams.Publisher
import org.reactivestreams.Subscriber
import org.reactivestreams.Subscription

/**
 * Provides a FilteredPublisher view on the underlying  Publisher
 */
class FilteredPublisher<T>(val predicate : (T) -> Boolean, val parentPublisher: Publisher<T>) : Publisher<T> {
    override fun subscribe(subscriber: Subscriber<in T>) {
        parentPublisher.subscribe(FilteredSubscriber(subscriber))
    }

    /**
     * Provides a FilteredSubscriber to the underlying Publisher which forwards events to the real subscriber as desired
     */
    inner class FilteredSubscriber(val subscriber: Subscriber<in T>) : Subscriber<T> {
        private lateinit var subscription: Subscription
        override fun onNext(event: T) {
            if (predicate(event)) subscriber.onNext(event)
            else subscription.request(1)
        }

        override fun onSubscribe(subscription: Subscription) {
            this.subscription = subscription
            subscriber.onSubscribe(subscription)
        }

        override fun onError(t: Throwable) = subscriber.onError(t)
        override fun onComplete() = subscriber.onComplete()
    }
}


fun <T> Publisher<T>.filter(predicate: (T) -> Boolean) = FilteredPublisher(predicate, this)
