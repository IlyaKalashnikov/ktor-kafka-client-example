package org.soup.cabbage.sour

import io.ktor.server.application.*
import io.ktor.server.response.*
import io.ktor.server.routing.*
import org.apache.kafka.clients.producer.KafkaProducer
import org.apache.kafka.clients.producer.ProducerRecord
import org.apache.kafka.streams.state.ReadOnlyWindowStore
import org.koin.ktor.ext.inject
import kotlin.random.Random

fun Application.clientExample() {

    val producer by inject<KafkaProducer<String, String>>()
    val storage by inject<ReadOnlyWindowStore<String,Long>>()

    routing {
        get("/produce") {
            val message = call.request.queryParameters["message"]
            producer.send(ProducerRecord("amazing-topic", Random(1337).nextInt().toString(), message))
            call.respondText("Send message $message to tour amazing topic")
        }

        get("/count") {
            val records = storage.all()
            val builder = StringBuilder()
            for (element in records) {
                builder.append("${element.key.key()} ${element.value} | ")
            }
            call.respondText { builder.toString() }
        }
    }
}