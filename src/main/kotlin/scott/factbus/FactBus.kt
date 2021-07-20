package scott.factbus

import com.fasterxml.jackson.module.kotlin.jacksonObjectMapper
import java.lang.reflect.ParameterizedType
import java.util.*
import java.util.UUID.randomUUID
import kotlin.reflect.full.memberProperties
import kotlin.reflect.jvm.javaField

data class Tracker(val id :  UUID, val name : String)
data class LogEntry(val key: String, val data: String?, val tracker: Tracker)

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

class LogView(private var log: Log = Log(), private var mysize: Int = log.length) {

    fun logEntries() : List<LogEntry> = log.data.subList(0, mysize)

    /*
     * append to the existing log or fork if we are back in time (undo)
     */
    fun append(entry: LogEntry) {
        if (mysize == log.length)
            log.append(entry) { mysize++ }
        else {
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

    fun materialize(): Map<String, LogEntry?> = logEntries().associateBy { entry -> entry.key }

    fun add(consumer: (LogEntry) -> Unit) = log.add(consumer)
}

data class FactTopic<T>(val url : String)

class Bus(private val logView: LogView = LogView()) {

    var map = logView.materialize()

    fun logEntries() : List<LogEntry> = logView.logEntries()

    fun add(action: Action) {
        val key = action.topic
        val data = action.data?.toJson()
        val newLogEntry = LogEntry(key = key, data = data, tracker = Tracker(action.id, action.name))
        val existingEntry = map[action.topic]
        if (existingEntry?.tracker != newLogEntry.tracker || existingEntry.data != newLogEntry.data) {
            logView.append(newLogEntry)
            map += (key to newLogEntry)
        }
    }

    inline operator fun <reified V> get(topic: FactTopic<V>): V? = map[topic.url]?.data?.parse<V>()

    fun back() = if (logView.back()) true.also { map = logView.materialize() } else false

    fun forward() = if (logView.forward()) true.also { map = logView.materialize() } else false

    fun subscribe(consumer: (LogEntry) -> Unit) = logView.add(consumer)
}


class Action(val id: UUID = randomUUID(), val name: String, val data: Any?, val topic : String)
fun action(data : Any, topic : String = getTopic(data), tracker : Tracker) = Action(tracker.id, tracker.name, data, topic = topic)
fun action(topic : String, tracker : Tracker) = Action(tracker.id, tracker.name, null, topic = topic)

class StreamItem<V>(val data : V?, val logEntry : LogEntry)

interface BStream<V> {
    val bus: Bus
    fun add(child : (StreamItem<V>) -> Unit)
    fun stream(data : StreamItem<V>)
}



class BTopicStream<V>(bus : Bus) : BStream<V> {
    override val bus: Bus = bus
    private var children = emptyList<(StreamItem<V>) -> Unit>()
    override fun add(child : (StreamItem<V>) -> Unit) {
        children += child
    }

    override fun stream(data: StreamItem<V>) {
        println("Streaming ${data.logEntry.key}  ${data.logEntry.data}")
        children.forEach { callChild ->  callChild(data) }
    }
}

class BTransformStream<VR,V>(source: BStream<V>, private val transformer: Bus.(V?) -> VR) : BStream<VR> {
    override val bus: Bus = source.bus
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
    override val bus: Bus = source.bus
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

inline fun <reified V1> Bus.stream(topic1: FactTopic<V1>): BStream<StreamData1<V1?>> {
    return BTopicStream<StreamData1<V1?>>(this).also {
        subscribe { logEntry ->
            if (logEntry.key == topic1.url)
                it.stream(StreamItem(logEntry = logEntry, data = StreamData1(logEntry.data?.parse<V1>())))
        }
    }
}

inline fun <reified V1,reified V2> Bus.stream(topic1: FactTopic<V1>, topic2: FactTopic<V2>): BStream<StreamData2<V1?,V2?>> {
    return BTopicStream<StreamData2<V1?,V2?>>(this).also {
        subscribe { logEntry ->
            if (logEntry.key == topic1.url)
                it.stream(StreamItem(data = StreamData2(logEntry.data?.parse<V1>(), get(topic2)), logEntry = logEntry))
            else if (logEntry.key == topic2.url)
                it.stream(StreamItem(data = StreamData2(get(topic1), logEntry.data?.parse<V2>()), logEntry = logEntry))
        }
    }
}

inline fun <reified V1,reified V2,reified V3> Bus.stream(topic1: FactTopic<V1>, topic2: FactTopic<V2>, topic3: FactTopic<V3>): BStream<StreamData3<V1?,V2?,V3?>> {
    return BTopicStream<StreamData3<V1?,V2?,V3?>>(this).also {
        subscribe { logEntry ->
            when {
                logEntry.key == topic1.url -> it.stream(StreamItem(data = StreamData3(logEntry.data?.parse<V1>(), get(topic2), get(topic3)), logEntry = logEntry))
                logEntry.key == topic2.url -> it.stream(StreamItem(data = StreamData3(get(topic1), logEntry.data?.parse<V2>(), get(topic3)), logEntry = logEntry))
                logEntry.key == topic3.url -> it.stream(StreamItem(data = StreamData3(get(topic1), get(topic2), logEntry.data?.parse<V3>()), logEntry = logEntry))
            }
        }
    }
}

fun <V, VR> BStream<V>.transform(transform: Bus.(V?) -> VR): BStream<VR> = BTransformStream(this, transform)

inline fun <V, reified V2, VR> BStream<V>.append(topic: FactTopic<V2>, crossinline appender: (V?, V2?) -> VR): BStream<VR> = BTransformStream(this) { value -> appender(value, bus[topic]) }

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


inline fun Any.toJson(): String {
    return jacksonObjectMapper().writeValueAsString(this)
}

operator fun <T> Pair<T, *>?.component1() = this?.component1()
operator fun <T> Pair<*, T>?.component2() = this?.component2()

operator fun <T> Triple<T,*,*>?.component1() = this?.component1()
operator fun <T> Triple<*,T,*>?.component2() = this?.component2()
operator fun <T> Triple<*,*,T>?.component3() = this?.component3()

interface TopicAggregate
data class TopicAggregate2<V1,V2>(val t1 : FactTopic<V1>, val t2 : FactTopic<V2>) : TopicAggregate


interface StreamData {
    fun from(topicAggregate : TopicAggregate)
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
