package scott.factbus.reactor

import org.reactivestreams.Publisher

fun <T> flux() : PublisherOps<T> = CorePublisher()

fun <T> List<Publisher<T>>.toMonoOfList() : Publisher<List<T>> = first().concat(*subList(1, size).toTypedArray()).collectList()

fun main(args: Array<String>) {

    val numberPublisher = CorePublisher<Int>()

    /**
     * subscribe to all numbers between 1 and 10, converting thenm to strings and printing them
     */
    numberPublisher.filter { it >= 1 && it <= 10 }
        .map { "Matched $it"  }
        .subscribe { println(it)}

    /**
     * subscribe to the collection of numbers from 1..10
     */
    (1..10).map { i ->
        numberPublisher.filter { it == i } //filter on publications of number I
            .next() //then take the next one for a (mono) single result
    }
    .toMonoOfList() // convert the List<Publisher<T>> to Publisher<List<T>> which publishes all data when "all the (mono) publishers complete"
    .subscribe { println("All numbers from 1 to 10 have been found!!!") } //subscribe and print


    /**
     * subscribe to the collection of numbers from 20..30
     */
    (20..30).map { i ->  numberPublisher.filter { it == i }.next() }.toMonoOfList().subscribe { println("All numbers from 20 to 30 have been found!!!") }



    /**
     * subscribe to 3 specific numbers
     */
    listOf(10, 35, 90).map { i ->  numberPublisher.filter { it == i }.next() }.toMonoOfList().subscribe { println("10, 35 and 90 have been found!!!") }


    /**
     * now make the numberPublisher emit some numbers
     */
    listOf(10, 35, 90).forEach { i ->numberPublisher.emitNext(i) }
    /*
    (1..5000).forEach {
        numberPublisher.emitNext((1..500).random())
    }*/

    (1..100).forEach { i ->
        numberPublisher.emitNext(i.also { println("EMIT $it") })
    }

}