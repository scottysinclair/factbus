package scott.factbus.reactor.operations

import org.reactivestreams.Publisher
import org.reactivestreams.Subscriber
import org.reactivestreams.Subscription

/**
 * Wraps a list of publishers as a single publisher
 * Subscribes and publishes data from each publisher in order, moving onto the next one
 * when the previous publisher completes
 */
class ConcatPublisher<T>(val publishers: List<Publisher<T>>) : Publisher<T> {
    override fun subscribe(subscriber: Subscriber<in T>) {
        ConcatSubscriber(subscriber, publishers).start()
    }

    inner class ConcatSubscriber(val subscriber: Subscriber<in T>, publishers: List<Publisher<T>>) : Subscriber<T>,
        Subscription {
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

fun <T> concat(publishers : List<Publisher<T>>) : Publisher<T> = ConcatPublisher(publishers)