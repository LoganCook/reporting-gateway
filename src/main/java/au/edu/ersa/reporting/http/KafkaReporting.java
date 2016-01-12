package au.edu.ersa.reporting.http;

import io.dropwizard.Application;
import io.dropwizard.auth.AuthFactory;
import io.dropwizard.auth.Authenticator;
import io.dropwizard.auth.CachingAuthenticator;
import io.dropwizard.auth.basic.BasicAuthFactory;
import io.dropwizard.auth.basic.BasicCredentials;
import io.dropwizard.setup.Bootstrap;
import io.dropwizard.setup.Environment;
import io.swagger.config.FilterFactory;
import io.swagger.core.filter.AbstractSpecFilter;
import io.swagger.jaxrs.config.BeanConfig;
import io.swagger.jaxrs.listing.ApiListingResource;
import io.swagger.model.ApiDescription;
import io.swagger.models.Operation;
import io.swagger.models.parameters.Parameter;

import java.io.File;
import java.io.IOException;
import java.util.EnumSet;
import java.util.List;
import java.util.Map;

import javax.servlet.DispatcherType;
import javax.servlet.FilterRegistration;

import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.retry.ExponentialBackoffRetry;
import org.eclipse.jetty.servlets.CrossOriginFilter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import au.edu.ersa.reporting.http.KafkaReportingConfiguration.AuthType;
import au.edu.ersa.reporting.http.KafkaReportingConfiguration.DynamoConfig;
import au.edu.ersa.reporting.http.health.PingHealthCheck;
import au.edu.ersa.reporting.http.health.ZKHealthCheck;
import au.edu.ersa.reporting.http.resources.BrokerResource;
import au.edu.ersa.reporting.http.resources.StatusResource;
import au.edu.ersa.reporting.http.resources.TopicResource;
import au.edu.ersa.reporting.http.resources.UserResource;
import au.edu.ersa.reporting.kzk.Kafka;
import au.edu.ersa.reporting.kzk.ZK;
import au.edu.ersa.reporting.security.AuthAlgorithm;
import au.edu.ersa.reporting.security.BasicAuth;
import au.edu.ersa.reporting.security.DynamoDBAuthenticator;
import au.edu.ersa.reporting.security.HMAC;
import au.edu.ersa.reporting.security.LocalFileAuthenticator;
import au.edu.ersa.reporting.security.User;

import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.regions.Region;
import com.amazonaws.regions.Regions;
import com.codahale.metrics.health.HealthCheck;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.google.common.base.CharMatcher;
import com.google.common.base.Joiner;
import com.google.common.base.Splitter;


public class KafkaReporting extends Application<KafkaReportingConfiguration> {
    private final static Logger LOG = LoggerFactory.getLogger(KafkaReporting.class);

    public static final int VERSION = 1;
    public static final String VERSION_PREFIX = "/v" + VERSION + "/";

    public static void main(String[] args) throws Exception {
        new KafkaReporting().run(args);
    }

    @Override
    public void initialize(Bootstrap<KafkaReportingConfiguration> config) {}

    private static final Splitter ZK_SPLITTER = Splitter.on(CharMatcher.BREAKING_WHITESPACE).omitEmptyStrings().trimResults();
    private static final Joiner ZK_JOINER = Joiner.on(',');

    private static class SwaggerFilter extends AbstractSpecFilter {
        @Override
        public boolean isParamAllowed(Parameter parameter, Operation operation, ApiDescription api, Map<String, List<String>> params, Map<String, String> cookies, Map<String, List<String>> headers) {
            String description = parameter.getDescription();

            return description == null || !description.equalsIgnoreCase("ignore");
        }
    }

    private static void addCORS(Environment env, String allowedOrigins) {
        FilterRegistration.Dynamic filter = env.servlets().addFilter("CORSFilter", CrossOriginFilter.class);
        filter.addMappingForUrlPatterns(EnumSet.of(DispatcherType.REQUEST), true, env.getApplicationContext().getContextPath() + "*");
        filter.setInitParameter(CrossOriginFilter.ALLOWED_METHODS_PARAM, "GET,PUT,POST,DELETE,OPTIONS");
        filter.setInitParameter(CrossOriginFilter.ALLOWED_ORIGINS_PARAM, allowedOrigins);
        filter.setInitParameter(CrossOriginFilter.ALLOWED_HEADERS_PARAM, "Origin, Content-Type, Accept, Authorization");
        filter.setInitParameter(CrossOriginFilter.ALLOW_CREDENTIALS_PARAM, "true");
    }

    @Override
    public void run(KafkaReportingConfiguration config, Environment env) throws Exception {
        // Swagger requirements.
        env.getObjectMapper().setSerializationInclusion(JsonInclude.Include.NON_NULL);
        FilterFactory.setFilter(new SwaggerFilter());

        // Tolerate a whitespace- or comma-separated ZK connection string.
        String zkConnectionString = ZK_JOINER.join(ZK_SPLITTER.split(config.getZkConnectionString()));

        CuratorFramework curator = CuratorFrameworkFactory.newClient(zkConnectionString, new ExponentialBackoffRetry(500, Integer.MAX_VALUE, 5));

        ZK zk = new ZK(curator);
        Kafka kafka = new Kafka(zk);

        AuthAlgorithm auth = new HMAC(config.getHexAuthKey());

        BasicAuth basicAuth = null;

        if (config.getAuthType().equals(AuthType.LOCAL)) {
            if (config.getLocalAuthFile() == null || config.getLocalAuthFile().isEmpty()) {
                throw new IOException("invalid configuration: local auth file missing");
            }

            basicAuth = new LocalFileAuthenticator(new File(config.getLocalAuthFile()), auth);
        } else if (config.getAuthType().equals(AuthType.DYNAMODB)) {
            DynamoConfig dc = config.getDynamoDB();
            basicAuth = new DynamoDBAuthenticator(auth,
                    new BasicAWSCredentials(dc.getAccess(), dc.getSecret()),
                    Region.getRegion(Regions.fromName(dc.getRegion())),
                    dc.getTable());
        } else {
            throw new Exception("no authentication provider specified");
        }

        env.lifecycle().manage(basicAuth);

        Authenticator<BasicCredentials,User> authenticator = basicAuth;
        if (config.getAuthCache() != null) {
            authenticator = new CachingAuthenticator<>(env.metrics(), basicAuth, config.getAuthCache());
        }

        env.jersey().register(AuthFactory.binder(new BasicAuthFactory<User>(authenticator, getClass().getSimpleName(), User.class)));

        // addCORS(env, "*");

        HealthCheck pingHealthCheck = new PingHealthCheck();
        HealthCheck zkHealthCheck = new ZKHealthCheck(zk);

        env.healthChecks().register("ping", pingHealthCheck);
        env.healthChecks().register("zk", zkHealthCheck);

        env.lifecycle().manage(zk);
        env.lifecycle().manage(kafka);

        env.jersey().register(new StatusResource(pingHealthCheck, zkHealthCheck));
        env.jersey().register(new TopicResource(kafka));
        env.jersey().register(new BrokerResource(kafka));
        env.jersey().register(new UserResource(basicAuth));

        env.jersey().register(new ApiListingResource());

        BeanConfig bean = new BeanConfig();
        bean.setTitle("Kafka Reporting");
        bean.setVersion(Integer.toString(KafkaReporting.VERSION));
        bean.setResourcePackage("au.edu.ersa.reporting.http.resources");
        bean.setScan(true);
    }
}
