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
            subscriber.onSubscribe(subscription)
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
        override fun onNext(event: SOURCE) {
            subscriber.onNext(mapper(event))
        }

        override fun onSubscribe(subscription: Subscription) {
            subscriber.onSubscribe(subscription)
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
//                println("MONO SUBSCRIBER NEXT $event")
                subscriber.onNext(event)
//                println("MONO SUBSCRIBER COMPLETE $event")
                subscriber.onComplete()
                subscription.cancel() //cancel because we have sent our single value
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

fun <T> Publisher<T>.next() = MonoPublisher(this)

/**
 * Provides a FlapMapPublisher view on an underlying Publisher
 */

class FlatMapPublisher<SOURCE,DEST>(val flatMapper: (SOURCE) -> Publisher<DEST>, val publisher: Publisher<SOURCE>) : Publisher<DEST> {
    override fun subscribe(subscriber: Subscriber<in DEST>) {
        publisher.subscribe(FlatMapSubscriber(subscriber))

    }

    inner class FlatMapSubscriber(val subscriber: Subscriber<in DEST>) : Subscriber<SOURCE> {
        override fun onNext(event: SOURCE) {
            /*
             * call flatMapper to get the Publisher<DEST> 'P'
             * then use 'map' to wrap 'P' so that P.onNext(DEST) will call ourSubscriber.onNext()
             */
            flatMapper(event).map { subscriber.onNext(it) }
        }

        override fun onSubscribe(subscription: Subscription) {
            subscriber.onSubscribe(subscription)
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

/**
 * Wraps a list of publishers as a single publisher
 * Subscribes and publishes data from each publisher in order, moving onto the next one
 * when the previous publisher completes
 */
class ConcatPublisher<T>(vararg val publishers: Publisher<T>) : Publisher<T> {
    override fun subscribe(subscriber: Subscriber<in T>) {
        ConcatSubscriber(subscriber, *publishers).start()
    }

    inner class ConcatSubscriber(val subscriber: Subscriber<in T>, vararg  publishers: Publisher<T>) : Subscriber<T>, Subscription {
        private val iterator = publishers.iterator()
        private lateinit var currentPublisher : Publisher<T>
        private lateinit var currentUnderlyingSub : Subscription
        private var lastNumberOfEventsRequested : Long = 0L

        fun start() {
            currentPublisher = iterator.next()
            currentPublisher.subscribe(this) //this as subscriber
            subscriber.onSubscribe(this)  //this as 'special concat' subscription
        }

        override fun onNext(event: T) {
            //println("CONCAT ONNEXT $event")
            subscriber.onNext(event)
        }

        override fun onSubscribe(subscription: Subscription) {
            currentUnderlyingSub = subscription
            if (lastNumberOfEventsRequested > 0) {
                currentUnderlyingSub.request(lastNumberOfEventsRequested)
            }
        }

        override fun onError(t: Throwable) {
            subscriber.onError(t)
        }

        override fun onComplete() {
            //the current publisher says we are complete, move on..
            if (iterator.hasNext()) {
                //println("CONCAT MOVING TO NEXT PUBLISHER")
                currentPublisher = iterator.next()
                currentPublisher.subscribe(this)
            }
            else {
                //no more publishers, we are really complete, so propagate it
                //println("CONCAT FINISHED")
                subscriber.onComplete()
            }
        }

        override fun request(numberOfEventsRequested: Long) {
            lastNumberOfEventsRequested = numberOfEventsRequested
            currentUnderlyingSub.request(numberOfEventsRequested)
        }

        override fun cancel() {
            currentUnderlyingSub.cancel()
        }
    }
}

/**
 * Collects all emitted data into a list which is published when the underlying publication completes
 */
class CollectPublisher<T>(val publisher: Publisher<T>) : Publisher<List<T>> {
    override fun subscribe(subscriber: Subscriber<in List<T>>) {
        publisher.subscribe(CollectPublisher(subscriber))
    }

    inner class CollectPublisher(val subscriber: Subscriber<in List<T>>) : Subscriber<T> {
        private val list = mutableListOf<T>()
        override fun onSubscribe(subscription: Subscription) {
            subscriber.onSubscribe(subscription)
        }

        override fun onNext(event: T) {
            list.add(event)
        }

        override fun onError(t: Throwable?) {
            subscriber.onError(t)
        }

        override fun onComplete() {
            subscriber.onNext(list)
            subscriber.onComplete()
        }
    }
}

fun <T> Publisher<T>.concat(vararg publishers : Publisher<T>) : Publisher<T> = ConcatPublisher(*publishers)

fun <T> Publisher<T>.collectList() : Publisher<List<T>> = CollectPublisher(this)

