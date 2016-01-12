package au.edu.ersa.reporting.http.api;

import com.fasterxml.jackson.annotation.JsonProperty;

public class Status {
    public Status(boolean status) {
        this.status = status;
    }

    private final boolean status;

    @JsonProperty
    public boolean getStatus() { return status; }
}
