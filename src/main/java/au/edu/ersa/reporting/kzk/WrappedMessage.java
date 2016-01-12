package au.edu.ersa.reporting.kzk;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonInclude.Include;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonRawValue;

@JsonInclude(Include.NON_NULL)
public class WrappedMessage {
    public final int partition;
    public final long offset;
    public final String topic;

    @JsonProperty("next_offset")
    public final long nextOffset;

    @JsonRawValue
    public final String message;

    public WrappedMessage(int partition, long offset, long nextOffset, String topic, String message) {
        this.partition = partition;
        this.offset = offset;
        this.nextOffset = nextOffset;
        this.topic = topic;
        this.message = message;
    }
}
