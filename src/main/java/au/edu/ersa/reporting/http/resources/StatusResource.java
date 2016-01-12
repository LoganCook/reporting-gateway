package au.edu.ersa.reporting.http.resources;

import io.swagger.annotations.Api;

import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;

import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.core.MediaType;

import au.edu.ersa.reporting.http.KafkaReporting;
import au.edu.ersa.reporting.http.api.Status;

import com.codahale.metrics.health.HealthCheck;

@Path(KafkaReporting.VERSION_PREFIX + "status")
@Api(value = "status")
@Produces(MediaType.APPLICATION_JSON)
public class StatusResource extends ReportingResource {
    private final Collection<HealthCheck> healthChecks;

    public StatusResource(HealthCheck ... healthChecks) {
        this.healthChecks = Arrays.asList(healthChecks);
    }

    public StatusResource(Collection<HealthCheck> healthChecks) {
        this.healthChecks = Collections.unmodifiableCollection(healthChecks);
    }

    @GET
    public Status get() {
        return new Status(healthChecks.stream().map(check -> check.execute()).allMatch(result -> result.isHealthy()));
    }
}
