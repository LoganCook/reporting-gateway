package au.edu.ersa.reporting.http.api;

import java.util.HashMap;
import java.util.Map;
import java.util.UUID;

public class ReportingResponse {
    public final Map<String,String> response = init();

    private static final Map<String,String> init() {
        Map<String,String> map = new HashMap<>();

        map.put("id", UUID.randomUUID().toString().toLowerCase());
        map.put("timestamp", Long.toString(System.currentTimeMillis()));

        return map;
    }
}
