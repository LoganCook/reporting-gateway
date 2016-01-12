package au.edu.ersa.reporting.http.api;

import java.util.Collections;
import java.util.List;

public class BrokerList extends ReportingResponse {
    public final List<String> brokers;

    public BrokerList(List<String> list) {
        if (list != null) {
            this.brokers = Collections.unmodifiableList(list);
        } else {
            this.brokers = Collections.emptyList();
        }
    }
}
