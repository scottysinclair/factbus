package scott.factbus

import java.util.UUID.randomUUID

object Facts {
    val allrooms = FactTopic<AllRooms>("fact://allrooms")
    val alltasks = FactTopic<AllTasks>("fact://alltasks")
    val availablerooms = FactTopic<AvailableRooms>("fact://availablerooms")
    val availabletasks = FactTopic<AvailableTasks>("fact://availabletasks")
    val chosentask = FactTopic<ChosenTask>("fact://chosentask")
    val chosenrooms = FactTopic<ChosenRooms>("fact://chosenrooms")
    val thetask = FactTopic<TheTask>("fact://thetask")
    val therooms = FactTopic<TheRooms>("fact://therooms")
}

class Question(val name: String,
               val options: () -> List<String>, //the topic where we find the options for the question
               val answer: (String) -> Unit, //the data of the answer (the question should be republished on the bus with it's answer)
               val factToAnswer: FactTopic<*> //the topic to publish the answer on in it'S own right
)


data class Room(val name: String, val possibleTasks: List<Task> = mutableListOf())
data class Task(val name: String)

data class AllRooms(val rooms: List<Room>)
data class AllTasks(val tasks: List<Task>)
data class AvailableRooms(val rooms: List<Room>)
data class AvailableTasks(val tasks: List<Task>)
data class ChosenTask(val task: Task)
data class ChosenRooms(val rooms: List<Room>)
data class TheTask(val task: Task)
data class TheRooms(val rooms: List<Room>)

fun main() {
    val bus = ScopedBus(scopeId = randomUUID())

    val `new windows` = Task("new windows")
    val `new doors` = Task("new doors")

    val `all tasks` = listOf(`new windows`, `new doors`).let { AllTasks(it) }

    val `living room` = Room("living room", listOf(`new windows`))
    val hall = Room("hall", listOf(`new doors`))
    val `all rooms` = listOf(`living room`, hall).let { AllRooms(it) }


    /**
     *  recalculate the rooms we can choose from
     */
    bus.stream(Facts.allrooms, Facts.chosentask)
            .transform { (allRooms, chosenTask) -> calculateAvailableRooms(allRooms, chosenTask) }
            .write(Facts.availablerooms)

    /**
     *  recalculate the tasks we can choose from
     */
    bus.stream(Facts.alltasks, Facts.chosenrooms)
            .transform { (allTasks, chosenRooms) -> calculateAvailableTasks(allTasks, chosenRooms) }
            .write(Facts.availabletasks)

    /**
     *  recalculate therooms when chosenrooms, allrooms or availabletasks changes
     */
    bus.stream(Facts.chosenrooms,Facts.allrooms, Facts.chosentask)
            .transform { (chosenRooms, allRooms, chosenTask) -> calculateTheRooms(allRooms, chosenTask, chosenRooms) }
            .write(Facts.therooms)

    /**
     *  recalculate the task to perform based on if it was chosen or if it is the only choice available
     */
    bus.stream(Facts.chosentask, Facts.availablerooms, Facts.therooms)
            .transform { (chosenTask, availableRooms, theRooms) -> calculateTheTask(availableRooms, chosenTask, theRooms) }
            .write(Facts.thetask)


    bus.add("all rooms", `all rooms`)
    bus.add("all tasks", `all tasks`)

    fun ScopedBus.getRoom(roomName : String) : Room = get(Facts.allrooms)!!.rooms.first { r -> r.name == roomName }
    val `which room do you want` = Question(
            name = "which room do you want?",
            options = { bus[Facts.availablerooms]?.rooms?.map(Room::name) ?: emptyList() },
            answer = { answer ->  bus.add("chosen rooms", ChosenRooms(listOf(bus.getRoom(answer)))) },
            factToAnswer = Facts.therooms
    )

    println()
    println()
    println("----------------------------------------------------------")
    println("                   BUS")
    bus.map().forEach { (k, v) -> println("$k  =>  ${v?.data}")}
    println()
    println("----------------------------------------------------------")
    println("                   LOG")
    bus.logEntries().forEach(::println)

    println(`which room do you want`.name)
    `which room do you want`.options().forEach { o ->
        println("    $o")
    }
    println(":_")
    println("CHOOSING 'LIVING ROOM'")
    `which room do you want`.answer("living room")

    println()
    println()
    println("----------------------------------------------------------")
    println("                   BUS")
    bus.map().forEach { (k, v) -> println("$k  =>  ${v?.data}")}
    println()
    println("----------------------------------------------------------")
    println("                   LOG")
    bus.logEntries().forEach(::println)

    println()
    println("Undo answering the question........................")
    bus.back()
    println()
    println("----------------------------------------------------------")
    println("                   BUS")
    bus.map().forEach { (k, v) -> println("$k  =>  ${v?.data}")}
    println()
    println("----------------------------------------------------------")
    println("                   LOG")
    bus.logEntries().forEach(::println)


    println("CHOOSING 'HALL'")
    `which room do you want`.answer("hall")

    println()
    println()
    println("----------------------------------------------------------")
    println("                   BUS")
    bus.map().forEach { (k, v) -> println("$k  =>  ${v?.data}")}
    println()
    println("----------------------------------------------------------")
    println("                   LOG")
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

fun calculateAvailableTasks(allTasks: AllTasks?, chosenRooms: ChosenRooms?) : AvailableTasks {
    return when (allTasks) {
        null -> emptyList()
        else -> when (chosenRooms) {
            null -> allTasks.tasks
            else ->  chosenRooms.rooms.flatMap(Room::possibleTasks).toSet().toList()
        }
    }.let { AvailableTasks(it) }

}

fun calculateTheTask(availableRooms: AvailableRooms?, chosenTask: ChosenTask?, theRooms: TheRooms?): TheTask? {
    return when (chosenTask) {
        null -> when (theRooms) {
            null -> when(availableRooms) {
                null -> null
                else -> availableRooms.rooms.flatMap { r -> r.possibleTasks }
                        .toSet()
                        .takeIf { it.size == 1 }?.let { TheTask(it.first()) }
            }
            else -> theRooms.rooms.flatMap { r -> r.possibleTasks }
                    .toSet()
                    .takeIf { it.size == 1 }?.let { TheTask(it.first()) }
        }
        else -> TheTask(chosenTask.task)
    }
}

fun calculateTheRooms(allRooms : AllRooms?, chosenTask: ChosenTask?, chosenRooms: ChosenRooms?): TheRooms? {
    return when(allRooms) {
        null -> null
        else -> when (chosenRooms) {
            null -> when (chosenTask) {
                null -> null
                /*
                 * if the chosen task forces exactly 1 room, then we can set 'the room'
                 */
                else -> allRooms.rooms.filter { r -> r.possibleTasks.contains(chosenTask.task)  }.let { rooms ->
                    rooms.takeUnless { it.size == 1 }?.let { TheRooms(it) }
                }
            }
            else -> TheRooms(chosenRooms.rooms)
        }
    }
}

fun ScopedBus.add(stepName : String, data : Any) {
    add(Action(
            name = stepName,
            data = data,
            topic = getTopic(data)
    ))
}
