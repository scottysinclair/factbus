package scott.factbus

import com.fasterxml.jackson.module.kotlin.jacksonObjectMapper
import java.util.*
import java.util.UUID.randomUUID
import kotlin.reflect.full.memberProperties

data class Tracker(val id :  UUID, val name : String)
data class LogEntry(val key: String, val data: String?, val tracker: Tracker)

class Log(
        private var entries: List<LogEntry> = emptyList(),
        private var consumers: List<(LogEntry) -> Unit> = emptyList()
) {
    val data: List<LogEntry>
        get() = entries

    fun append(entry: LogEntry) {
        entries += (entry)
        consumers.forEach { consume -> consume(entry) }
    }

    fun add(consumer: (LogEntry) -> Unit) {
        consumers += consumer
    }

    /**
     * create a new log so that pos is the most recent value
     */
    fun forkAt(pos: Int) = Log(entries.subList(0, pos + 1), consumers = consumers)

    val length: Int
        get() = entries.size
}

class LogView(private var log: Log = Log(), private var pos: Int = log.length) {

    fun logEntries() : List<LogEntry> = log.data.subList(0, pos)

    /*
     * append to the existing log or fork if we are back in time (undo)
     */
    fun append(entry: LogEntry) {
        if (pos == log.length)
            log.append(entry)
        else {
            log = log.forkAt(pos)
            log.append(entry)

        }
        pos++
    }

    /**
     * back to the previous tracker
     */
    fun back(): Boolean {
        return if (pos > 0) {
            val lastTracker = log.data[pos - 1].tracker
            while (pos > 0 && log.data[--pos].tracker == lastTracker);
            pos++
            true
        } else false
    }

    /**
     * forward to the next tracker
     */
    fun forward(): Boolean {
        return if (pos < log.data.size) {
            val lastTracker = log.data[pos - 1].tracker
            while (pos < log.data.size && log.data[++pos].tracker == lastTracker);
            pos++
            true
        } else false
    }

    fun materialize(): Map<String, String?> = logEntries().map { entry -> entry.key to entry.data }.toMap()

    fun add(consumer: (LogEntry) -> Unit) = log.add(consumer)
}


class Bus(private val logView: LogView = LogView()) {

    var map = logView.materialize()

    fun logEntries() : List<LogEntry> = logView.logEntries()

    fun add(action: Action) {
        val key = action.topic
        val data = action.data?.toJson()
        logView.append(LogEntry(key = key, data = data, tracker = Tracker(action.id, action.name)))
        map += (key to data)
    }

    inline operator fun <reified V> get(keyAndType: Pair<String, Class<V>>): V? = map[keyAndType.first]?.parse<V>()

    fun back() = if (logView.back()) true.also { map = logView.materialize() } else false

    fun forward() = if (logView.forward()) true.also { map = logView.materialize() } else false

    fun subscribe(consumer: (LogEntry) -> Unit) = logView.add(consumer)
}


class Action(val id: UUID = randomUUID(), val name: String, val data: Any?, val topic : String)
fun action(data : Any, topic : String = getTopic(data), tracker : Tracker) = Action(tracker.id, tracker.name, data, topic = topic)
fun action(topic : String, tracker : Tracker) = Action(tracker.id, tracker.name, null, topic = topic)

class StreamData<V>(val data : V?, val tracker : Tracker)

interface BStream<V> {
    val bus: Bus
    fun add(child : (StreamData<V>) -> Unit)
    fun stream(data : StreamData<V>)
}

class BTopicStream<V>(private val type : Class<V>, override val bus: Bus, private val topic: String) : BStream<V> {
    private var children = emptyList<(StreamData<V>) -> Unit>()
    init {
        bus.subscribe { logEntry ->
            if (logEntry.key == topic) {
                println("BTopicStream processing ${logEntry.key}")
                stream(StreamData(data = logEntry.data?.parse(type), tracker =  logEntry.tracker))
            }
//            else println("BTopicStream $topic: ignoring ${logEntry.key}")
        }
    }

    override fun add(child : (StreamData<V>) -> Unit) {
        children += child
    }

    override fun stream(data: StreamData<V>) {
        children.forEach { callChild ->  callChild(data) }
    }
}

class BTransformStream<VR,V>(source: BStream<V>, private val transformer: Bus.(V?) -> VR) : BStream<VR> {
    override val bus: Bus = source.bus
    private var children = emptyList<(StreamData<VR>) -> Unit>()
    init {
        source.add { streamData ->
            println("BTransformStream: received ${streamData.data}")
            bus.transformer(streamData.data).let {
                stream(StreamData(data = it, tracker = streamData.tracker))
            }
        }
    }

    override fun add(child : (StreamData<VR>) -> Unit) {
        children += child
    }

    override fun stream(data: StreamData<VR>) {
        children.forEach { callChild ->  callChild(data) }
    }

}

class BWriteStream<V>(source: BStream<V>, private val topic: String) : BStream<V> {
    override val bus: Bus = source.bus
    init {
        source.add { streamData ->
            println("BWriteStream: received ${streamData.data}")
            when(streamData.data) {
                null -> bus.add(action(topic = topic, tracker = streamData.tracker))
                else -> bus.add(action(topic = topic, data = streamData.data, tracker = streamData.tracker))
            }
        }
    }
    override fun add(child : (StreamData<V>) -> Unit) { /* NOOP */ }
    override fun stream(data: StreamData<V>) { /* NOOP */ }
}

inline fun <reified V> Bus.stream(topicAndType: Pair<String, Class<V>>): BStream<V> = BTopicStream(V::class.java, this, topicAndType.first)

fun <V, VR> BStream<V>.transform(transform: Bus.(V?) -> VR): BStream<VR> = BTransformStream(this, transform)

inline fun <V, reified V2, reified VR> BStream<V>.append(topicAndType: Pair<String, Class<V2>>, crossinline appender: (V?, V2?) -> VR): BStream<VR> = BTransformStream(this) { value -> appender(value, bus[topicAndType]) }


fun <V> BStream<V>.write(topicAndType: Pair<String, Class<V>>) {
    BWriteStream(this, topicAndType.first)
}

fun getTopic(fact: Any): String {
    return Facts::class.memberProperties.fold(emptyList<String>()) { acc, prop ->
        (prop.invoke(Facts) as Pair<String, Class<*>>).let { propValue ->
            when {
                propValue.second == fact::class.java -> acc + propValue.first
                else -> acc
            }
        }

    }.first()
}

object Facts {
    val allrooms = "fact://allrooms" to AllRooms::class.java
    val alltasks = "fact://alltasks" to AllTasks::class.java
    val availablerooms = "fact://availablerooms" to AvailableRooms::class.java
    val chosentask = "fact://chosentask" to ChosenTask::class.java

    /*
    val availabletasks = "fact://availabletasks"
    val thetask = "fact://thetask"
    val therooms = "fact://therooms"
    val chosenrooms = "fact://chosenrooms"

     */
}

fun Bus.add(data : Any) {
    add(Action(
            name = "n/a",
            data = data,
            topic = getTopic(data)
    ))
}


fun main() {

    /*
      all rooms
      all tasks
      available rooms
      available tasks
      chosen task
      chosen room
      the task
      the room
     */

    val bus = Bus()

    val `new windows` = Task("new windows")
    val `new doors` = Task("new doors")

    val `all tasks` = listOf(`new windows`, `new doors`).let { AllTasks(it) }.also {
        bus.add(it)
    }

    val `living room` = Room("living room", listOf(`new windows`))
    val hall = Room("hall", listOf(`new doors`))

    val `all rooms` = listOf(`living room`, hall).let { AllRooms(it) }.also {
        bus.add(it)
    }

    /*
    val `ask what rooms` = Question(
            name = "what rooms",
            options = "availablerooms://",
            answer = null,
            factToAnswer = "fact://what rooms",
    ).also {
        bus.add(it)
    }

    val `ask what task` = Question(
            name = "what task",
            options = "availabletasks://",
            answer = null,
            factToAnswer = "fact://what task",
    ).also {
        bus.add(it)
    }
*/


    /**
     *  a stream of 'allrooms' fact changes which recalculates the available rooms for choosing
     */
    bus.stream(Facts.allrooms)
            .append(Facts.chosentask) { allRooms, chosenTask -> allRooms to chosenTask }
            .transform { (allRooms, chosenTask) -> calculateAvailableRooms(allRooms, chosenTask) }
            .write(Facts.availablerooms)

    /**
     *  a stream of 'chosentask' fact changes which recalculates the available rooms for choosing
     */
    bus.stream(Facts.chosentask)
            .append(Facts.allrooms) { chosenTask, allRooms -> allRooms to chosenTask }
            .transform { (allRooms, chosenTask) -> calculateAvailableRooms(allRooms, chosenTask) }
            .write(Facts.availablerooms)


    println("----------------------------------------------------------")
    println("                   READY BUS")
    bus.map.forEach { (k, v) -> println("$k  =>  $v")}
    println()
    println("----------------------------------------------------------")
    println("                   READY LOG")
    bus.logEntries().forEach(::println)

    println()
    println()


    ChosenTask(`new doors`).also {
        bus.add(Action(
                name = "choose task",
                data = it,
                topic = getTopic(it)
        ))
    }
    println("------------  RESULT ---------------------------------------")
    println("Available Rooms after:")
    bus[Facts.availablerooms]?.rooms?.forEach { print("    $it") }
    println()
    println("----------------------------------------------------------")
    println("                   LOG                                    ")
    bus.logEntries().forEach(::println)

    println()
    println()

    bus.back()
    println("------------  BACK RESULT ---------------------------------------")
    println("Available Rooms after:")
    bus[Facts.availablerooms]?.rooms?.forEach { print("    $it") }
    println()
    println("----------------------------------------------------------")
    println("                   BACK LOG                                    ")
    bus.logEntries().forEach(::println)

    println()
    println()

    bus.forward()
    println("------------  FORWARD RESULT ---------------------------------------")
    println("Available Rooms after:")
    bus[Facts.availablerooms]?.rooms?.forEach { print("    $it") }
    println()
    println("----------------------------------------------------------")
    println("                   FORWARD LOG                                    ")
    bus.logEntries().forEach(::println)

    println()
    println()

    bus.back()
    println("------------  BACK RESULT ---------------------------------------")
    println("Available Rooms after:")
    bus[Facts.availablerooms]?.rooms?.forEach { print("    $it") }
    println()
    println("----------------------------------------------------------")
    println("                   BACK LOG                                    ")
    bus.logEntries().forEach(::println)


    println("----------------------------------------------------------")
    println("                   CHOSSING SOMETHING DIFFERENT - FORKING THE LOG!!!!               ")
    println("----------------------------------------------------------")
    ChosenTask(`new windows`).also {
        bus.add(Action(
                name = "choose task",
                data = it,
                topic = getTopic(it)
        ))
    }

    println("------------  FORK RESULT ---------------------------------------")
    println("Available Rooms after:")
    bus[Facts.availablerooms]?.rooms?.forEach { print("    $it") }
    println()
    println("----------------------------------------------------------")
    println("                   FORK LOG                                    ")
    bus.logEntries().forEach(::println)

}

fun calculateAvailableRooms(allRooms: AllRooms?, chosenTask: ChosenTask?): AvailableRooms {
    return when (allRooms) {
        null -> emptyList()
        else -> when (chosenTask) {
            null -> allRooms.rooms
            else -> allRooms.rooms.filter { r -> r.possibleTasks.contains(chosenTask.task) }
        }
    }.let { AvailableRooms(it) }
}


class Question(name: String,
               val options: String, //the topic where we find the options for the question
               val answer: Any?, //the data of the answer (the question should be republished on the bus with it's answer)
               val factToAnswer: String //the topic to publish the answer on in it'S own right
)


data class Room(val name: String, val possibleTasks: List<Task> = mutableListOf())
data class Task(val name: String)

data class AllRooms(val rooms: List<Room>)
data class AllTasks(val tasks: List<Task>)
data class AvailableRooms(val rooms: List<Room>)
data class ChosenTask(val task: Task)


inline fun <reified T> String.parse(): T {
  return jacksonObjectMapper().readValue(this, T::class.java)
}

inline fun <T> String.parse(type : Class<T>): T {
    return jacksonObjectMapper().readValue(this, type)
}


inline fun Any.toJson(): String {
    return jacksonObjectMapper().writeValueAsString(this)
}

operator fun <T> Pair<T, *>?.component1() = this?.component1()
operator fun <T> Pair<*, T>?.component2() = this?.component2()
