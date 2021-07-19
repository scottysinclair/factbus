package scott.factbus

import com.fasterxml.jackson.module.kotlin.jacksonObjectMapper
import java.util.*
import java.util.UUID.randomUUID
import kotlin.reflect.full.memberProperties

data class LogEntry(val key : String, val data : String, val tracker : Pair<UUID, String>)

class Log(var data: List<LogEntry> = emptyList()) {

    fun append(entry: LogEntry) {
        data += (entry)
    }

    /**
     * create a new log so that pos is the most recent value
     */
    fun forkAt(pos: Int) = Log(data.subList(0, pos + 1))

    val length: Int
        get() = data.size
}

class LogView(private var log: Log = Log(), private var pos: Int = log.length) {
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
    }

    /**
     * back to the previous tracker
     */
    fun back() : Boolean {
        return if (pos > 0) {
            val lastTracker = log.data[pos - 1].tracker
            while (pos > 0 && log.data[--pos].tracker == lastTracker);
            true
        }
        else false
    }

        /**
         * forward to the next tracker
         */
        fun forward() : Boolean {
            return if (pos < log.data.size) {
                val lastTracker = log.data[pos - 1].tracker
                while (pos < log.data.size && log.data[++pos].tracker == lastTracker);
                true
            }
            else false
        }

        fun materialize() : Map<String,String> = log.data.map { entry -> entry.key to entry.data }.toMap()
    }
}


class Bus(private val logView: LogView = LogView()) {

    private var map = logView.materialize()

    fun add(action : Action) {
        val key = getTopic(action.data)
        val data = action.data.toJson()
        logView.append(LogEntry(key = key, data = data, tracker = action.id to action.name))
        map += (key to data)
    }

    operator fun <V> get(keyAndType: Pair<String, Class<V>>): V? = map[keyAndType.first] as V

    fun back() = if (logView.back()) true.also { map = logView.materialize() } else false

    fun forward() = if (logView.forward()) true.also { map = logView.materialize() } else false
}


class Action(val id : UUID = randomUUID(), val name : String, val data : Any)

interface BStream<V> {
    val bus: Bus
}

class BTopicStream<V>(override val bus: Bus, private val topic: String) : BStream<V> {
    init {
        bus.subscribe(this)

    }
}

class BTransformStream<V, VR>(private val stream: BStream<V>, private val transformer: Bus.(V) -> VR) : BStream<VR> {
    override val bus: Bus = stream.bus
}

class BWriteStream<V>(private val stream: BStream<V>, private val toic: String) : BStream<V> {
    override val bus: Bus = stream.bus
}

fun <V> Bus.stream(topicAndType: Pair<String, Class<V>>): BStream<V> = BTopicStream(this, topicAndType.first)

fun <V, VR> BStream<V>.transform(transform: Bus.(V) -> VR): BStream<VR> = BTransformStream(this, transform)

fun <V, V2, VR> BStream<V>.append(topicAndType: Pair<String, Class<V2>>, appender : (V, V2?) -> VR): BStream<VR> = BTransformStream(this) { value -> appender(value, bus[topicAndType]) }


fun <V> BStream<V>.write(topicAndType: Pair<String, Class<V>>) {
    BWriteStream(this, topicAndType.first)
}

fun getTopic(fact : Any) : String {
    return Facts::class.memberProperties.fold(emptyList<String>()) { acc, prop ->
        (prop.invoke(Facts) as Pair<String,Class<*>>).let { propValue ->
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

}

fun calculateAvailableRooms(allRooms: AllRooms?, chosenTask: ChosenTask?): AvailableRooms {
    return when(allRooms) {
        null -> emptyList()
        else -> when(chosenTask) {
            null -> allRooms.rooms
            else -> allRooms.rooms.filter { r -> r.possibleTasks.contains(chosenTask) }
        }
    }.let { AvailableRooms(it) }
}



class Question(name: String,
               val options: String, //the topic where we find the options for the question
               val answer: Any?, //the data of the answer (the question should be republished on the bus with it's answer)
               val factToAnswer: String //the topic to publish the answer on in it'S own right
)


class Room(val name: String, val possibleTasks: List<Task> = mutableListOf())
class Task(val name: String)

data class AllRooms(val rooms: List<Room>)
data class AllTasks(val tasks: List<Task>)
data class AvailableRooms(val rooms: List<Room>)
data class ChosenTask(val name : String)


inline fun <reified T> String.parse(): T {
    return jacksonObjectMapper().readValue(this, T::class.java)
}

inline fun Any.toJson(): String {
    return jacksonObjectMapper().writeValueAsString(this)
}

