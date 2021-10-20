package scott.factbus.reactor

import org.reactivestreams.Publisher
import org.reactivestreams.Subscriber
import org.reactivestreams.Subscription
import java.util.concurrent.atomic.AtomicBoolean

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
        }

        override fun onError(t: Throwable) {
            subscriber.onError(t)
        }

        override fun onComplete() {
            subscriber.onComplete()
        }
    }
}


fun <T> Publisher<T>.filter(predicate: (T) -> Boolean) = FilteredPublisher(predicate, this)

/**
 * Provides a MappedPublisher view on the underlying Publisher which publishes mapped data
 */
class MappedPublisher<SOURCE,DEST>(val mapper : (SOURCE) -> DEST, val parentPublisher: Publisher<SOURCE>) : Publisher<DEST> {
    override fun subscribe(subscriber: Subscriber<in DEST>) {
        parentPublisher.subscribe(MappedSubscriber(subscriber))
    }

    /**
     * MappedSubscriber which allows Subscribers of type DEST to received transformed data from Publisher of type SOURCE
     */
    inner class MappedSubscriber(val subscriber: Subscriber<in DEST>) : Subscriber<SOURCE> {
        private lateinit var subscription: Subscription
        override fun onNext(event: SOURCE) {
            subscriber.onNext(mapper(event))
        }

        override fun onSubscribe(subscription: Subscription) {
            this.subscription = subscription
        }

        override fun onError(t: Throwable) {
            subscriber.onError(t)
        }

        override fun onComplete() {
            subscriber.onComplete()
        }
    }
}

fun <SOURCE, DEST> Publisher<SOURCE>.map(mapper: (SOURCE) -> DEST) = MappedPublisher(mapper,this)

/**
 * Provides a MonoPublisher view on an underlying Publisher
 */
class MonoPublisher<T>(val parentPublisher: Publisher<T>) : Publisher<T> {
    override fun subscribe(subscriber: Subscriber<in T>) {
        parentPublisher.subscribe(MonoSubscriber(subscriber))

    }

    inner class MonoSubscriber(val subscriber: Subscriber<in T>) : Subscriber<T> {
        private lateinit var subscription: Subscription
        private val terminated = AtomicBoolean(false)
        override fun onNext(event: T) {
            if (!terminated.compareAndExchange(false, true)) {
                subscriber.onNext(event)
                onComplete()
                subscription.cancel() //cancel because we have sent our single value
            }
        }

        override fun onSubscribe(subscription: Subscription) {
            this.subscription = subscription
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

fun <T> Publisher<T>.next() = MonoPublisher(this)

/**
 * Provides a FlapMapPublisher view on an underlying Publisher
 */

class FlatMapPublisher<SOURCE,DEST>(val flatMapper: (SOURCE) -> Publisher<DEST>, val publisher: Publisher<SOURCE>) : Publisher<DEST> {
    override fun subscribe(subscriber: Subscriber<in DEST>) {
        publisher.subscribe(FlatMapSubscriber(subscriber))

    }

    inner class FlatMapSubscriber(val subscriber: Subscriber<in DEST>) : Subscriber<SOURCE> {
        private lateinit var subscription: Subscription
        override fun onNext(event: SOURCE) {
            /*
             * call flatMapper to get the Publisher<DEST> 'P'
             * then use 'map' to wrap 'P' so that P.onNext(DEST) will call ourSubscriber.onNext()
             */
            flatMapper(event).map { subscriber.onNext(it) }
        }

        override fun onSubscribe(subscription: Subscription) {
            this.subscription = subscription
        }

        override fun onError(t: Throwable) {
            subscriber.onError(t)
        }

        override fun onComplete() {
            subscriber.onComplete()
        }
    }
}

fun <T,DEST> Publisher<T>.flatMap(mapper : (T) -> Publisher<DEST>) : Publisher<DEST> = FlatMapPublisher(mapper, this)