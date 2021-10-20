package scott.factbus.reactor

import org.reactivestreams.Publisher

fun <T> flux() : PublisherOps<T> = CorePublisher()

fun <T> List<Publisher<T>>.toMonoOfList() : Publisher<List<T>> = first().concat(*subList(1, size).toTypedArray()).collectList()

fun main(args: Array<String>) {

    val numberPublisher = CorePublisher<Int>()

    numberPublisher.filter { it >= 1 && it <= 10 }
        .map { "Matched $it"  }
        .subscribe { println(it)}

    (1..10).map { i ->  numberPublisher.filter { it == i }.next() }.toMonoOfList().subscribe { println("All numbers from 1 to 10 have been found!!!") }

    (20..30).map { i ->  numberPublisher.filter { it == i }.next() }.toMonoOfList().subscribe { println("All numbers from 20 to 30 have been found!!!") }

    listOf(10, 35, 90).map { i ->  numberPublisher.filter { it == i }.next() }.toMonoOfList().subscribe { println("10, 35 and 90 have been found!!!") }

    listOf(10, 35, 90).forEach { i ->numberPublisher.emitNext(i) }
/*
    (1..5000).forEach {
        numberPublisher.emitNext((1..500).random())
    }
    */

    /*
    (1..100).forEach { i ->
        numberPublisher.emitNext(i.also { println("EMIT $it") })
    }*/

}