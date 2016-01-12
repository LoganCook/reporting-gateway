package au.edu.ersa.reporting.http.api;

public class Broker extends ReportingResponse {
    // {"jmx_port":-1,"timestamp":"1411618536058","host":"foo.bar.com","version":1,"port":9092}

    public int id, jmxPort, version, port;
    public long timestamp;
    public String host;
}
