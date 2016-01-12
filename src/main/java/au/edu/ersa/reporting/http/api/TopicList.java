package au.edu.ersa.reporting.http.api;

import java.util.Collections;
import java.util.List;

public class TopicList extends ReportingResponse {
    public final List<String> topics;

    public TopicList(List<String> list) {
        if (list != null) {
            this.topics = Collections.unmodifiableList(list);
        } else {
            this.topics = Collections.emptyList();
        }
    }
}
