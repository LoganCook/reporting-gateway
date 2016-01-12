package au.edu.ersa.reporting.http.resources;

import io.dropwizard.auth.Auth;
import io.swagger.annotations.Api;
import io.swagger.annotations.ApiParam;

import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ForkJoinPool;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import javax.servlet.http.HttpServletRequest;
import javax.ws.rs.Consumes;
import javax.ws.rs.DefaultValue;
import javax.ws.rs.GET;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.QueryParam;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.HttpHeaders;
import javax.ws.rs.core.MediaType;

import org.apache.kafka.clients.producer.RecordMetadata;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import au.edu.ersa.reporting.http.KafkaReporting;
import au.edu.ersa.reporting.http.api.Message;
import au.edu.ersa.reporting.http.api.Topic;
import au.edu.ersa.reporting.http.api.TopicList;
import au.edu.ersa.reporting.http.api.TopicMessageMap;
import au.edu.ersa.reporting.http.api.WrappedMessageList;
import au.edu.ersa.reporting.kzk.Kafka;
import au.edu.ersa.reporting.kzk.WrappedMessage;
import au.edu.ersa.reporting.security.User;

import com.google.common.base.Joiner;

@Path(KafkaReporting.VERSION_PREFIX + "topic")
@Api(value = "topic")
@Consumes(MediaType.APPLICATION_JSON)
@Produces(MediaType.APPLICATION_JSON)
public class TopicResource extends ReportingResource {
    private final static Logger LOG = LoggerFactory.getLogger(TopicResource.class);

    private final Kafka kafka;

    public TopicResource(Kafka kafka) {
        this.kafka = kafka;
    }

    @GET
    public TopicList list(@Auth @ApiParam("ignore") User user) {
        return new TopicList(kafka.listTopics());
    }

    @GET
    @Path("/{id}")
    public Topic get(@Auth @ApiParam("ignore") User user, @PathParam("id") String id) {
        require(user.canRead(id), () -> LOG.warn("denied read from {} by {}", id, user.id));

        Topic topic = kafka.getTopic(id);

        if (topic != null) {
            return topic;
        } else {
            throw NOT_FOUND;
        }
    }

    @GET
    @Path("/{id}/{partition}/{offset}")
    public WrappedMessageList get(@Auth @ApiParam("ignore") User user, @PathParam("id") String id, @PathParam("partition") int partition, @PathParam("offset") long offset, @QueryParam("maxBytes") @DefaultValue("0") int maxBytes) {
        require(user.canRead(id), () -> LOG.warn("denied read from {} by {}", id, user.id));

        List<WrappedMessage> messages = maxBytes <= 0 ? kafka.fetch(id, partition, offset) : kafka.fetch(id, partition, offset, maxBytes);

        if (messages != null) {
            return new WrappedMessageList(messages);
        } else {
            throw NOT_FOUND;
        }
    }

    private static void validate(Collection<Message> messages) {
        boolean valid = true;

        for (Message message : messages) {
            List<String> violations = message.validate();
            if (!violations.isEmpty()) {
                valid = false;
                LOG.warn("invalid message: " + message.id + " / " + violations);
            }
        }

        if (!valid) {
            throw BAD_REQUEST;
        }
    }

    private static void handleFutures(Map<String,Future<RecordMetadata>> futures) {
        ForkJoinPool pool = new ForkJoinPool(Math.min(futures.size(), 16));
        List<String> failedMessages = null;
        Exception outrightFailure = null;

        try {
            failedMessages = pool.submit(() -> {
                return futures.entrySet().stream().parallel().map(entry -> {
                    try {
                        entry.getValue().get(30, TimeUnit.SECONDS);
                        return null;
                    } catch (Exception e) {
                        LOG.warn("message insertion error: {}", e);
                        return entry.getKey();
                    }
                }).filter(x -> x != null).collect(Collectors.toList());
            }).get();
        } catch (Exception e) {
            outrightFailure = e;
        } finally {
            pool.shutdown();
        }

        if (outrightFailure != null) {
            LOG.error("failed to retrieve message insertion futures!", outrightFailure);
            throw INTERNAL_SERVER_ERROR;
        } else if (!failedMessages.isEmpty()) {
            String messages = Joiner.on(' ').join(failedMessages);
            throw BAD_REQUEST(messages);
        }
    }

    private void populateMessages(Collection<Message> messages, HttpServletRequest request) {
        populateMessages(messages, System.currentTimeMillis(), request);
    }

    private void populateMessages(Collection<Message> messages, long timestamp, HttpServletRequest request)  {
        messages.stream().forEach(message -> populateMessage(message, timestamp, request));
    }

    private void populateMessage(Message message, long timestamp, HttpServletRequest request) {
        message.timestamp = timestamp;
        message.source = request.getRemoteAddr();

        String userAgent = request.getHeader(HttpHeaders.USER_AGENT);
        message.userAgent = userAgent != null ? userAgent : "";
    }

    @POST
    @Path("/{id}")
    public void post(@Auth @ApiParam("ignore") User user, @Context HttpServletRequest request, @PathParam("id") String id, @QueryParam("sync") @DefaultValue("true") boolean sync, List<Message> messages) {
        messages.stream().forEach(message -> {
            require(user.canWrite(id), () -> LOG.warn("denied write to {} by {}", id, user.id));
        });

        populateMessages(messages, request);

        validate(messages);

        Map<String,Future<RecordMetadata>> futures = kafka.insert(id, messages);

        if (sync) {
            handleFutures(futures);
        }
    }

    @POST
    public void post(@Auth @ApiParam("ignore") User user, @Context HttpServletRequest request, @QueryParam("sync") @DefaultValue("true") boolean sync, TopicMessageMap map) {
        for (Map.Entry<String,List<Message>> entry : map.messages.entrySet()) {
            String topic = entry.getKey();
            List<Message> messages = entry.getValue();

            messages.stream().forEach(message -> {
                require(user.canWrite(topic), () -> LOG.warn("denied write to {} by {}", topic, user.id));
            });
        }

        map.messages.values().stream().forEach(messages -> {
            populateMessages(messages, request);
            validate(messages);
        });

        Map<String,Future<RecordMetadata>> futures = kafka.insert(map);

        if (sync) {
            handleFutures(futures);
        }
    }
}
