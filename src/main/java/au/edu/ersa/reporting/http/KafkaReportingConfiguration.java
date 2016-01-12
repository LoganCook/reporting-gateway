package au.edu.ersa.reporting.http;

import io.dropwizard.Configuration;

import javax.validation.constraints.NotNull;

import org.hibernate.validator.constraints.NotEmpty;

import com.fasterxml.jackson.annotation.JsonFormat;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.cache.CacheBuilderSpec;

public class KafkaReportingConfiguration extends Configuration {
    @NotEmpty
    private String zkConnectionString;

    @JsonProperty
    public String getZkConnectionString() {
        return zkConnectionString;
    }

    private CacheBuilderSpec authCache;

    @JsonProperty
    public CacheBuilderSpec getAuthCache() {
        return authCache;
    }

    @JsonFormat(shape = JsonFormat.Shape.OBJECT)
    public static enum AuthType {
        LOCAL("local"), DYNAMODB("dynamodb");

        private final String text;

        private AuthType(String text) {
            this.text = text;
        }

        @Override
        public String toString() { return text; }
    }

    @NotNull
    private AuthType authType;

    @JsonProperty
    public AuthType getAuthType() {
        return authType;
    }

    private String localAuthFile;

    @JsonProperty
    public String getLocalAuthFile() {
        return localAuthFile;
    }

    @NotEmpty
    private String hexAuthKey;

    @JsonProperty
    public String getHexAuthKey() {
        return hexAuthKey;
    }

    public static class DynamoConfig extends Configuration {
        private String region, table, access, secret;

        @JsonProperty
        public String getRegion() {
            return region;
        }

        @JsonProperty
        public String getTable() {
            return table;
        }

        @JsonProperty
        public String getAccess() {
            return access;
        }

        @JsonProperty
        public String getSecret() {
            return secret;
        }
    }

    private DynamoConfig dynamoDB;

    @JsonProperty(value = "dynamodb")
    public DynamoConfig getDynamoDB() {
        return dynamoDB;
    }
}
