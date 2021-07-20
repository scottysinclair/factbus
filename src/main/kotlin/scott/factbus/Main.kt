package scott.factbus

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
data class AvailableTasks(val tasks: List<Task>)
data class ChosenTask(val task: Task)
data class ChosenRooms(val rooms: List<Room>)
data class TheTask(val task: Task)
data class TheRooms(val rooms: List<Room>)

fun main() {
    val bus = Bus()

    val `new windows` = Task("new windows")
    val `new doors` = Task("new doors")

    val `all tasks` = listOf(`new windows`, `new doors`).let { AllTasks(it) }

    val `living room` = Room("living room", listOf(`new windows`))
    val hall = Room("hall", listOf(`new doors`))
    val `all rooms` = listOf(`living room`, hall).let { AllRooms(it) }


    /**
     *  recalculate available rooms when allrooms or chosentask changes
     */
    bus.stream(Facts.allrooms, Facts.chosentask)
            .transform { (allRooms, chosenTask) -> calculateAvailableRooms(allRooms, chosenTask) }
            .write(Facts.availablerooms)


    /**
     *  recalculate available tasks when alltasks or chosenrooms changes
     */
    bus.stream(Facts.alltasks, Facts.chosenrooms)
            .transform { (allTasks, chosenRooms) -> calculateAvailableTasks(allTasks, chosenRooms) }
            .write(Facts.availabletasks)

    /**
     *  recalculate thetask when chosentask or availablerooms changes
     */
    bus.stream(Facts.chosentask, Facts.availablerooms)
            .transform { (chosenTask, availableRooms) -> calculateTheTask(availableRooms, chosenTask) }
            .write(Facts.thetask)

    /**
     *  recalculate therooms when chosenrooms, allrooms or availabletasks changes
     */
    bus.stream(Facts.chosenrooms,Facts.allrooms, Facts.chosentask)
            .transform { (chosenRooms, allRooms, chosenTask) -> calculateTheRooms(allRooms, chosenTask, chosenRooms) }
            .write(Facts.therooms)

    /*
     * populate
     */
    bus.add("all rooms", `all rooms`)
    bus.add("all tasks", `all tasks`)


    println("----------------------------------------------------------")
    println("                   READY BUS")
    bus.map.forEach { (k, v) -> println("$k  =>  ${v?.data}")}
    println()
    println("----------------------------------------------------------")
    println("                   READY LOG")
    bus.logEntries().forEach(::println)

    println()
    println()


    bus.add("choose new doors", ChosenTask(`new doors`))

    println("------------  RESULT ---------------------------------------")
    println("Available Rooms after:")
    bus[Facts.availablerooms]?.rooms?.forEach { print("    $it") }
    println()
    println("The Task: ${bus[Facts.thetask]}")
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
    println("The Task: ${bus[Facts.thetask]}")
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
    println("The Task: ${bus[Facts.thetask]}")
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
    println("The Task: ${bus[Facts.thetask]}")
    println("----------------------------------------------------------")
    println("                   BACK LOG                                    ")
    bus.logEntries().forEach(::println)


    println("----------------------------------------------------------")
    println("                   CHOoSING SOMETHING DIFFERENT - FORKING THE LOG!!!!               ")
    println("----------------------------------------------------------")

    bus.add("choose new windows", ChosenTask(`new windows`))

    println("------------  FORK RESULT ---------------------------------------")
    println("Available Rooms after:")
    bus[Facts.availablerooms]?.rooms?.forEach { print("    $it") }
    println()
    println("The Task: ${bus[Facts.thetask]}")
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

fun calculateAvailableTasks(allTasks: AllTasks?, chosenRooms: ChosenRooms?) : AvailableTasks {
    return when (allTasks) {
        null -> emptyList()
        else -> when (chosenRooms) {
            null -> allTasks.tasks
            else ->  chosenRooms.rooms.flatMap(Room::possibleTasks).toSet().toList()
        }
    }.let { AvailableTasks(it) }

}

fun calculateTheTask(availableRooms: AvailableRooms?, chosenTask: ChosenTask?): TheTask? {
    return when (chosenTask) {
        null -> when (availableRooms) {
            null -> null
            else -> availableRooms.rooms.flatMap { r -> r.possibleTasks }
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
                else -> allRooms.rooms.filter { r -> r.possibleTasks.contains(chosenTask.task)  }.let { rooms ->
                    rooms.takeUnless { it.isEmpty() }?.let { TheRooms(it) }
                }
            }
            else -> TheRooms(chosenRooms.rooms)
        }
    }
}

fun Bus.add(stepName : String, data : Any) {
    add(Action(
            name = stepName,
            data = data,
            topic = getTopic(data)
    ))
}
