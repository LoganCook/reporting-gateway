package au.edu.ersa.reporting.http.resources;

import io.dropwizard.auth.Auth;
import io.swagger.annotations.Api;
import io.swagger.annotations.ApiParam;

import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.core.MediaType;

import au.edu.ersa.reporting.http.KafkaReporting;
import au.edu.ersa.reporting.http.api.Broker;
import au.edu.ersa.reporting.http.api.BrokerList;
import au.edu.ersa.reporting.kzk.Kafka;
import au.edu.ersa.reporting.security.User;

@Path(KafkaReporting.VERSION_PREFIX + "broker")
@Api(value = "broker")
@Produces(MediaType.APPLICATION_JSON)
public class BrokerResource extends ReportingResource {
    private final Kafka kafka;

    public BrokerResource(Kafka kafka) {
        this.kafka = kafka;
    }

    @GET
    public BrokerList getAll(@Auth @ApiParam("ignore") User user) {
        return new BrokerList(kafka.listBrokers());
    }

    @GET
    @Path("/{id}")
    public Broker get(@Auth @ApiParam("ignore") User user, @PathParam("id") String id) {
        Broker broker = kafka.getBroker(id);

        if (broker != null) {
            return broker;
        } else {
            throw NOT_FOUND;
        }
    }
}
