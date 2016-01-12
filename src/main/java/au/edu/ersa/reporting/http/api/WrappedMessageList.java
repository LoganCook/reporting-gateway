package au.edu.ersa.reporting.http.api;

import java.util.Collections;
import java.util.List;

import au.edu.ersa.reporting.kzk.WrappedMessage;

public class WrappedMessageList extends ReportingResponse {
    public final List<WrappedMessage> messages;

    public WrappedMessageList(List<WrappedMessage> list) {
        if (list != null) {
            this.messages = Collections.unmodifiableList(list);
        } else {
            this.messages = Collections.emptyList();
        }
    }
}
