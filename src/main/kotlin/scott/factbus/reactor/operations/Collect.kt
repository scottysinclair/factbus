package scott.factbus.reactor.operations

import org.reactivestreams.Publisher
import org.reactivestreams.Subscriber
import org.reactivestreams.Subscription

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

        override fun onError(t: Throwable) = subscriber.onError(t)

        override fun onComplete() {
            subscriber.onNext(list)
            subscriber.onComplete()
        }
    }
}

fun <T> Publisher<T>.collectList() : Publisher<List<T>> = CollectPublisher(this)

