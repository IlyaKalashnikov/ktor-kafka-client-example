package org.soup.cabbage.sour

import io.ktor.server.application.*
import org.apache.kafka.clients.admin.NewTopic
import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.kafka.clients.producer.ProducerConfig
import org.apache.kafka.common.serialization.StringSerializer
import org.apache.kafka.streams.StoreQueryParameters
import org.apache.kafka.streams.StreamsBuilder
import org.apache.kafka.streams.kstream.*
import org.apache.kafka.streams.state.QueryableStoreTypes
import org.apache.kafka.streams.state.ReadOnlyWindowStore
import org.koin.dsl.module
import org.koin.ktor.plugin.Koin
import org.sour.cabbage.soup.Kafka
import org.sour.cabbage.soup.KafkaStreamsConfig
import org.sour.cabbage.soup.kafkaStreams
import org.sour.cabbage.soup.producer
import java.time.Duration
import java.util.*

const val BOOTSTRAP_SERVERS = "localhost:29092"
const val TOPIC = "amazing-topic"

fun main(args: Array<String>): Unit =
    io.ktor.server.netty.EngineMain.main(args)

@Suppress("unused") // application.conf references the main function. This annotation prevents the IDE from marking it as unused.
fun Application.module() {
    clientExample()

    install(Kafka) {
        this.kafkaConfig = mapOf(
            ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG to BOOTSTRAP_SERVERS
        )
        this.topics = listOf(NewTopic(TOPIC, 1, 1))
    }

    install(Koin) {
        val producer = producer<String, String>(
            mapOf(
                ProducerConfig.BOOTSTRAP_SERVERS_CONFIG to BOOTSTRAP_SERVERS,
                ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG to StringSerializer::class.java,
                ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG to StringSerializer::class.java
            )
        )

        val streams = kafkaStreams(
            KafkaStreamsConfig(
                topologyBuilder = topology(),
                streamsConfig = streamsConfig(),
                builder = StreamsBuilder()
            )
        )
        streams.cleanUp()
        streams.start()


        environment.monitor.subscribe(ApplicationStopped) {
            streams.close(Duration.ofSeconds(5))
        }

        val storage : ReadOnlyWindowStore<String, Long> =
            streams.store(
                StoreQueryParameters.fromNameAndType(
                    "windowed-word-count",
                    QueryableStoreTypes.windowStore()
                )
            )

        val kafkaModule = module {
            single { producer }
            single { streams }
            single { storage }
        }

        modules(kafkaModule)
    }
}
