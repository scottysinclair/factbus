package scott.factbus.reactor

fun <T> flux() : PublisherOps<T> = CorePublisher()

