package au.edu.ersa.reporting.http.api;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import javax.validation.constraints.Min;
import javax.validation.constraints.NotNull;
import javax.validation.constraints.Pattern;

import org.hibernate.validator.constraints.NotEmpty;

import au.edu.ersa.reporting.http.Util;
import au.edu.ersa.reporting.http.Wrap;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.databind.PropertyNamingStrategy;
import com.fasterxml.jackson.databind.annotation.JsonNaming;

@JsonNaming(PropertyNamingStrategy.LowerCaseWithUnderscoresStrategy.class)
public class Message {
    @JsonProperty
    @Pattern(regexp = "^\\p{XDigit}{8}-\\p{XDigit}{4}-\\p{XDigit}{4}-\\p{XDigit}{4}-\\p{XDigit}{12}$")
    public String session;

    @JsonProperty
    @Pattern(regexp = "^\\p{XDigit}{8}-\\p{XDigit}{4}-\\p{XDigit}{4}-\\p{XDigit}{4}-\\p{XDigit}{12}$")
    public String id;

    @JsonProperty
    @Min(1)
    public long timestamp;

    @JsonProperty
    @Min(1)
    public int version;

    @JsonProperty
    @NotEmpty
    public String schema;

    @JsonProperty
    @NotEmpty
    public String source;

    @JsonProperty
    @NotNull
    public String userAgent;

    @JsonProperty
    @NotNull
    public Map<?,?> data;

    @Override
    public String toString() { return Util.toJSON(this); }

    public static Message fromJSON(String json) {
        return Wrap.runtimeException(() -> Util.JSON.readValue(json, Message.class));
    }

    private static final Map<String,List<Class<?>>> DATA_REQUIREMENTS;

    static {
        Map<String,List<Class<?>>> dataRequirements = new HashMap<>();

        dataRequirements.put("hostname", Arrays.asList(String.class));
        dataRequirements.put("timestamp", Arrays.asList(Integer.class, Long.class));

        DATA_REQUIREMENTS = Collections.unmodifiableMap(dataRequirements);
    }

    public List<String> validate() {
        List<String> violations = Util.VALIDATOR.validate(this).stream().map(v -> v.getPropertyPath() + ": " + v.getMessage()).collect(Collectors.toList());

        for (Map.Entry<String,List<Class<?>>> requirement : DATA_REQUIREMENTS.entrySet()) {
            if (!data.containsKey(requirement.getKey())) {
                violations.add(String.format("absent: data[%s]", requirement.getKey()));
            } else if (!requirement.getValue().contains(data.get(requirement.getKey()).getClass())) {
                violations.add(String.format("invalid type: data[%s] must be in %s", requirement.getKey(), requirement.getValue().stream().map(c -> c.getSimpleName()).collect(Collectors.toList())));
            }
        }

        return violations;
    }
}
