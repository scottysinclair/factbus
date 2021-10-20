package scott.factbus.reactor.operations

import org.reactivestreams.Publisher
import org.reactivestreams.Subscriber
import org.reactivestreams.Subscription

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
            subscriber.onNext(mapper(event)) //subscriber gets the transformed value
        }

        override fun onSubscribe(subscription: Subscription) {
            subscriber.onSubscribe(subscription)
        }

        override fun onError(t: Throwable) = subscriber.onError(t)
        override fun onComplete() = subscriber.onComplete()
    }
}

fun <SOURCE, DEST> Publisher<SOURCE>.map(mapper: (SOURCE) -> DEST) = MappedPublisher(mapper,this)
