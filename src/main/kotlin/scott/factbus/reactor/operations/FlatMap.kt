package scott.factbus.reactor.operations

import org.reactivestreams.Publisher
import org.reactivestreams.Subscriber
import org.reactivestreams.Subscription
import scott.factbus.reactor.core.subscribe

/**
 * Provides a FlapMapPublisher view on an underlying Publisher
 */
class FlatMapPublisher<SOURCE,DEST>(val flatMapper: (SOURCE) -> Publisher<DEST>, val publisher: Publisher<SOURCE>) : Publisher<DEST> {
    override fun subscribe(subscriber: Subscriber<in DEST>) {
        publisher.subscribe(FlatMapSubscriber(subscriber))
    }

    inner class FlatMapSubscriber(val subscriber: Subscriber<in DEST>) : Subscriber<SOURCE> {
        override fun onNext(event: SOURCE) {
            //TODO: I guess we never need to cancel this dynamic subscription,
            //TODO: I guess we expect the underlying Publisher (which receives the subscription) to complete it normally when there is no more data, sounds reasonable
            flatMapper(event).subscribe { ev ->  subscriber.onNext(ev) }
        }

        override fun onSubscribe(subscription: Subscription) {
            subscriber.onSubscribe(subscription)
        }

        override fun onError(t: Throwable) = subscriber.onError(t)
        override fun onComplete() = subscriber.onComplete()
    }
}

fun <T,DEST> Publisher<T>.flatMap(mapper : (T) -> Publisher<DEST>) : Publisher<DEST> = FlatMapPublisher(mapper, this)