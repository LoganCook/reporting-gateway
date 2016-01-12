package au.edu.ersa.reporting.http;

import javax.validation.Validation;
import javax.validation.Validator;

import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;

public class Util {
    public static final Validator VALIDATOR = Validation.buildDefaultValidatorFactory().getValidator();
    public static final ObjectMapper JSON = new ObjectMapper().enable(DeserializationFeature.FAIL_ON_READING_DUP_TREE_KEY);

    public static boolean anyAreNull(Object... objects) {
        for (Object o : objects) {
            if (o == null) { return true; }
        }
        return false;
    }

    public static String toJSON(Object o) {
        return Wrap.runtimeException(() -> JSON.writeValueAsString(o));
    }
}
