package scott.factbus.reactor

import org.reactivestreams.Publisher
import org.reactivestreams.Subscriber
import org.reactivestreams.Subscription
import java.util.concurrent.atomic.AtomicBoolean
import kotlin.math.min

interface PublisherOps<T>

class CorePublisher<T> : Publisher<T>, PublisherOps<T> {
    private val subs = mutableListOf<CoreSubscription<T>>()
    private val completed = AtomicBoolean(false)

    override fun subscribe(subscriber: Subscriber<in T>) {
        synchronized(subs) { CoreSubscription(this::cancelSub, subscriber).also { subs.add(it) } }
            .also { subscriber.onSubscribe(it) }
    }

    fun emitNext(event : T) {
        if (!completed.get()) {
            synchronized(subs) { subs.toTypedArray() }.forEach { it.publish(event) }
         }
    }

    private fun cancelSub(sub : Subscription) {
        synchronized(subs) { subs.remove(sub) }
    }
}

 class CoreSubscription<T>(val cancelSub : (Subscription) -> Unit, val subscriber : Subscriber<in T>) : Subscription {
    private var subscribersCapacity = 0L
    private val queue = mutableListOf<T>()
    private val terminated = AtomicBoolean(false)

     fun publish(event : T) {
         if (!terminated.get()) queue.add(event)
         drain()
     }

     private fun drain() {
         queue.syncExtractMax(subscribersCapacity).forEach { subscriber.onNext(it) }
     }

     override fun request(numberOfEventsRequested: Long) {
         if (!terminated.get()) {
             subscribersCapacity += numberOfEventsRequested
         }
    }

    override fun cancel() {
        if (!terminated.compareAndExchange(false, true)) {
            subscriber.onComplete()
            cancelSub(this)
        }
    }
}


fun <T> MutableList<T>.syncExtractMax(max : Long) : MutableList<T> = synchronized(this) {
    subList(0, min(size, max.roundDownToMaxInt())).also { subList -> removeAll(subList)  }
}

fun Long.roundDownToMaxInt() = min(Int.MAX_VALUE.toLong(), this).toInt()