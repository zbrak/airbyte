package io.airbyte.cdk.core.destination.async.model

import com.fasterxml.jackson.annotation.JsonProperty
import com.fasterxml.jackson.annotation.JsonPropertyDescription
import io.airbyte.protocol.models.v0.AirbyteMessage
import java.util.Objects

class PartialAirbyteMessage {

    @JsonProperty("type")
    @JsonPropertyDescription("Message type")
    var type: AirbyteMessage.Type? = null

    @JsonProperty("record")
    var record: PartialAirbyteRecordMessage? = null

    @JsonProperty("state")
    var state: PartialAirbyteStateMessage? = null

    @JsonProperty("serialized")
    var serialized: String? = null

    fun withType(type: AirbyteMessage.Type?): PartialAirbyteMessage {
        this.type = type
        return this
    }

    fun withRecord(record: PartialAirbyteRecordMessage?): PartialAirbyteMessage {
        this.record = record
        return this
    }

    fun withState(state: PartialAirbyteStateMessage?): PartialAirbyteMessage {
        this.state = state
        return this
    }

    /**
     * For record messages, this stores the serialized data blob (i.e.
     * `Jsons.serialize(message.getRecord().getData())`). For state messages, this stores the
     * _entire_ message (i.e. `Jsons.serialize(message)`).
     *
     *
     * See
     * [io.airbyte.cdk.core.destination.async.AsyncStreamConsumer.deserializeAirbyteMessage]
     * for the exact logic of how this field is populated.
     */
    fun withSerialized(serialized: String?): PartialAirbyteMessage {
        this.serialized = serialized
        return this
    }

    override fun equals(other: Any?): Boolean {
        if (this === other) {
            return true
        }
        if (other == null || javaClass != other.javaClass) {
            return false
        }
        val that = other as PartialAirbyteMessage
        return type == that.type && record == that.record && state == that.state && serialized == that.serialized
    }

    override fun hashCode(): Int {
        return Objects.hash(type, record, state, serialized)
    }

    override fun toString(): String {
        return "PartialAirbyteMessage{" +
                "type=" + type +
                ", record=" + record +
                ", state=" + state +
                ", serialized='" + serialized + '\'' +
                '}'
    }
}