package au.edu.ersa.reporting.security;

import io.dropwizard.auth.basic.BasicCredentials;

public interface AuthAlgorithm {
    public boolean isValid(BasicCredentials credentials, User user);

    public String generateSecret(String plaintext);
}
