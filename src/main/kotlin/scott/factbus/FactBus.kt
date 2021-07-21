package scott.factbus

import com.fasterxml.jackson.module.kotlin.jacksonObjectMapper
import java.lang.reflect.ParameterizedType
import java.util.*
import java.util.UUID.randomUUID
import kotlin.reflect.full.memberProperties
import kotlin.reflect.jvm.javaField

data class Tracker(val id :  UUID, val name : String)
data class LogEntry(val scope : String, val key: String, val data: String?, val tracker: Tracker)

/**
 * A Log which can be appended to and can notify consumers of new records.
 */
class Log(
        private var entries: List<LogEntry> = emptyList(),
        private var consumers: List<(LogEntry) -> Unit> = emptyList()
) {
    val data: List<LogEntry>
        get() = entries

    fun append(entry: LogEntry, callback: () -> Unit) {
        println("Writing ${entry.key} ${entry.data}")
        entries += (entry)
        callback()
        consumers.forEach { consume -> consume(entry) }
    }

    fun add(consumer: (LogEntry) -> Unit) {
        consumers += consumer
    }

    /**
     * create a new log so that pos is the most recent value
     */
    fun forkAt(pos: Int) = Log(entries.subList(0, pos), consumers = consumers)

    val length: Int
        get() = entries.size
}

/**
 * A View of a log which can go back and forward in event-time
 * Appending to the logview when back in time will cause the Log to get forked.
 */
class LogView(private var log: Log = Log(), private var mysize: Int = log.length) {

    fun logEntries() : List<LogEntry> = log.data.subList(0, mysize)

    /*
     * append to the existing log or fork if we are back in time (undo)
     */
    fun append(entry: LogEntry) {
        if (mysize == log.length)
            log.append(entry) { mysize++ }
        else {
            /**
             * TODO: if we share with X and  X.mysize > mysize then NO FORK ALLOWED (can't change shared history)
             * TODO: remove consumers from the old log
             */
            log = log.forkAt(mysize)
            log.append(entry) { mysize++ }
        }
    }

    /**
     * back to the previous tracker
     */
    fun back(): Boolean {
        return if (mysize > 0) {
            val lastTracker = log.data[mysize - 1].tracker
            while (mysize > 0 && log.data[mysize - 1].tracker == lastTracker) mysize--
            true
        } else false
    }

    /**
     * forward to the next tracker
     */
    fun forward(): Boolean {
        return if (mysize < log.data.size) {
            val lastTracker = log.data[mysize].tracker
            while (mysize < log.data.size && log.data[mysize].tracker == lastTracker) mysize++
            true
        } else false
    }

    fun add(consumer: (LogEntry) -> Unit) = log.add(consumer)
}

data class FactTopic<T>(val url : String)

/**
 * A bus abstraction of a Materialized LogView allowing get, put and subscribe operations
 */
class Bus(private val logView: LogView = LogView()) {

    var map = logView.materialize()

    fun logEntries() : List<LogEntry> = logView.logEntries()

    inline operator fun <reified V> get(key : String): V? = map[key]?.data?.parse<V>()

    fun add(newLogEntry : LogEntry) {
        val existingEntry = map[newLogEntry.key]
        //need to prevent possible infinite ping pong - check if the addition is redundant (the task <-> the room)
        if ((existingEntry?.data == null && newLogEntry.data == null).not() && (existingEntry?.tracker != newLogEntry.tracker || existingEntry.data != newLogEntry.data)) {
            logView.append(newLogEntry)
            map += ("${newLogEntry.scope}:${newLogEntry.key}" to newLogEntry)
        }
    }

    fun back() = if (logView.back()) true.also { map = logView.materialize() } else false

    fun forward() = if (logView.forward()) true.also { map = logView.materialize() } else false

    fun subscribe(consumer: (LogEntry) -> Unit) = logView.add(consumer)

    private fun LogView.materialize() = logEntries().associateBy { entry -> "${entry.scope}:${entry.key}" }
}

class ScopedBus(val bus : Bus = Bus(), val scopeId : UUID = randomUUID()) {
    inline operator fun <reified V> get(topic: FactTopic<V>): V? = bus.get<V>("${scopeId}:${topic.url}")

    fun add(action: Action) {
        bus.add(LogEntry(
                scope = "$scopeId",
                key = action.topic,
                data = action.data?.toJson(),
                tracker = Tracker(action.id, action.name)))
    }

    fun back() = bus.back()

    fun forward() = bus.forward()

    fun map() : Map<String,LogEntry?> = bus.map.entries.associate { (k,v) -> k.substring(scopeId.toString().length + 1) to v }

    fun logEntries() : List<LogEntry> = bus.logEntries()

    fun subscribe(consumer: (LogEntry) -> Unit) = bus.subscribe(consumer)
}


class Action(val id: UUID = randomUUID(), val name: String, val scopeId : UUID? = null, val data: Any?, val topic : String)
fun action(data : Any, topic : String = getTopic(data), tracker : Tracker) = Action(tracker.id, tracker.name, null, data, topic = topic)
fun action(topic : String, tracker : Tracker) = Action(tracker.id, tracker.name, null, null, topic = topic)

class StreamItem<V>(val data : V?, val logEntry : LogEntry)

interface BStream<V> {
    val bus: ScopedBus
    fun add(child : (StreamItem<V>) -> Unit)
    fun stream(data : StreamItem<V>)
}


class BTopicStream<V>(override val bus : ScopedBus) : BStream<V> {
    private var children = emptyList<(StreamItem<V>) -> Unit>()
    override fun add(child : (StreamItem<V>) -> Unit) {
        children += child
    }

    override fun stream(data: StreamItem<V>) {
        println("Streaming ${data.logEntry.key}  ${data.logEntry.data}")
        children.forEach { callChild ->  callChild(data) }
    }
}

class BTransformStream<VR,V>(source: BStream<V>, private val transformer: ScopedBus.(V?) -> VR) : BStream<VR> {
    override val bus: ScopedBus = source.bus
    private var children = emptyList<(StreamItem<VR>) -> Unit>()
    init {
        source.add { streamData ->
//            println("BTransformStream: received ${streamData.data}")
            bus.transformer(streamData.data).let {
                stream(StreamItem(data = it, logEntry = streamData.logEntry))
            }
        }
    }

    override fun add(child : (StreamItem<VR>) -> Unit) {
        children += child
    }

    override fun stream(data: StreamItem<VR>) {
        children.forEach { callChild ->  callChild(data) }
    }

}

class BWriteStream<V>(source: BStream<V>, private val topic: String) : BStream<V> {
    override val bus: ScopedBus = source.bus
    init {
        source.add { streamData ->
            when(streamData.data) {
                null -> bus.add(action(topic = topic, tracker = streamData.logEntry.tracker))
                else -> bus.add(action(topic = topic, data = streamData.data, tracker = streamData.logEntry.tracker))
            }
        }
    }
    override fun add(child : (StreamItem<V>) -> Unit) { /* NOOP */ }
    override fun stream(data: StreamItem<V>) { /* NOOP */ }
}

inline fun <reified V1> ScopedBus.stream(topic1: FactTopic<V1>): BStream<StreamData1<V1?>> {
    return BTopicStream<StreamData1<V1?>>(this).also {
        subscribe { logEntry ->
            if (logEntry.key == topic1.url)
                it.stream(StreamItem(logEntry = logEntry, data = StreamData1(logEntry.data?.parse<V1>())))
        }
    }
}

inline fun <reified V1,reified V2> ScopedBus.stream(topic1: FactTopic<V1>, topic2: FactTopic<V2>): BStream<StreamData2<V1?,V2?>> {
    return BTopicStream<StreamData2<V1?,V2?>>(this).also {
        subscribe { logEntry ->
            scopeId.toString().let { scope ->
                when {
                    scope  == logEntry.scope &&logEntry.key == topic1.url -> it.stream(StreamItem(data = StreamData2(logEntry.data?.parse<V1>(), get(topic2)), logEntry = logEntry))
                    scope  == logEntry.scope && logEntry.key == topic2.url -> it.stream(StreamItem(data = StreamData2(get(topic1), logEntry.data?.parse<V2>()), logEntry = logEntry))
                }
            }
        }
    }
}

inline fun <reified V1,reified V2,reified V3> ScopedBus.stream(topic1: FactTopic<V1>, topic2: FactTopic<V2>, topic3: FactTopic<V3>): BStream<StreamData3<V1?,V2?,V3?>> {
    return BTopicStream<StreamData3<V1?,V2?,V3?>>(this).also {
        subscribe { logEntry ->
            scopeId.toString().let { scope ->
                when {
                    scope  == logEntry.scope && logEntry.key == topic1.url -> it.stream(StreamItem(data = StreamData3(logEntry.data?.parse<V1>(), get(topic2), get(topic3)), logEntry = logEntry))
                    scope  == logEntry.scope && logEntry.key == topic2.url -> it.stream(StreamItem(data = StreamData3(get(topic1), logEntry.data?.parse<V2>(), get(topic3)), logEntry = logEntry))
                    scope  == logEntry.scope && logEntry.key == topic3.url -> it.stream(StreamItem(data = StreamData3(get(topic1), get(topic2), logEntry.data?.parse<V3>()), logEntry = logEntry))
                }
            }
        }
    }
}

fun <V, VR> BStream<V>.transform(transform: ScopedBus.(V?) -> VR): BStream<VR> = BTransformStream(this, transform)

fun <V> BStream<V>.write(topic: FactTopic<out V>) {
    BWriteStream(this, topic.url)
}

fun getTopic(fact: Any): String {
    return Facts::class.memberProperties.fold(emptyList<String>()) { acc, prop ->
            when {
                (prop.javaField?.genericType as? ParameterizedType)?.actualTypeArguments!![0] == fact::class.java -> acc + (prop.invoke(Facts) as FactTopic<*>).url
                else -> acc
            }
    }.first()
}


inline fun <reified T> String.parse(): T {
  return jacksonObjectMapper().readValue(this, T::class.java)
}

fun Any.toJson(): String {
    return jacksonObjectMapper().writeValueAsString(this)
}

class StreamData1<V1>(val v1 : V1) {
    operator fun component1() = this.v1
}
operator fun <T> StreamData1<T>?.component1() = this?.component1()

data class StreamData2<V1,V2>(val v1 : V1, val v2 : V2)
operator fun <T> StreamData2<T,*>?.component1() = this?.component1()
operator fun <T> StreamData2<*,T>?.component2() = this?.component2()

data class StreamData3<V1,V2,V3>(val v1 : V1, val v2 : V2, val v3 : V3)
operator fun <T> StreamData3<T,*,*>?.component1() = this?.component1()
operator fun <T> StreamData3<*,T,*>?.component2() = this?.component2()
operator fun <T> StreamData3<*,*,T>?.component3() = this?.component3()
