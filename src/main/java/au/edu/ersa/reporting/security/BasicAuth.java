package au.edu.ersa.reporting.security;

import io.dropwizard.auth.Authenticator;
import io.dropwizard.auth.basic.BasicCredentials;
import io.dropwizard.lifecycle.Managed;

import java.security.SecureRandom;
import java.util.List;
import java.util.Random;
import java.util.stream.IntStream;

import au.edu.ersa.reporting.security.ACL.Permission;

public abstract class BasicAuth implements Managed, Authenticator<BasicCredentials,User> {
    private static final String READ_ONLY = "read-only auth mechanism";
    private static final RuntimeException READ_ONLY_EXCEPTION = new RuntimeException(READ_ONLY);
    private static final Random RANDOM = new SecureRandom();

    public static final int DEFAULT_TOKEN_LENGTH = 16;

    protected final AuthAlgorithm auth;

    public BasicAuth(AuthAlgorithm auth) {
        this.auth = auth;
    }

    public AuthAlgorithm getAuthAlgorithm() { return auth; }

    @Override
    public void start() throws Exception {}

    @Override
    public void stop() throws Exception {}

    public boolean setAdmin(String id, boolean isAdmin) {
        throw READ_ONLY_EXCEPTION;
    }

    public User createUser(String id) {
        throw READ_ONLY_EXCEPTION;
    }

    public User updateUser(User user) {
        throw READ_ONLY_EXCEPTION;
    }

    public boolean deleteUser(String id) {
        throw READ_ONLY_EXCEPTION;
    }

    public User resetCredentials(String id) {
        throw READ_ONLY_EXCEPTION;
    }

    public void setACL(String id, String topic, Permission permission) {
        throw READ_ONLY_EXCEPTION;
    }

    public void deleteACL(String id, String topic) {
        throw READ_ONLY_EXCEPTION;
    }

    public abstract User getUser(String id);

    public abstract ACL getACL(String id, String topic);

    public boolean canPerform(String id, String topic, Permission permission) {
        ACL acl = getACL(id, topic);

        if (acl != null) {
            return acl.getPermission().contains(permission.toString());
        } else {
            return false;
        }
    }

    public abstract List<User> getAllUsers();

    public abstract int getUserCount();

    private static String generator(char c1, char c2) {
        return IntStream.rangeClosed(c1, c2).mapToObj(c -> Character.toString((char)c)).reduce((x, y) -> x + y).get();
    }

    private static final char[] VALID_CHARACTERS = (generator('a', 'z') + generator('0', '9')).toCharArray();

    protected static String generateRandomString() { return generateRandomString(DEFAULT_TOKEN_LENGTH); }

    protected static String generateRandomString(int length) {
        char[] result = new char[length];

        for (int i = 0; i < length; i++) {
            result[i] = VALID_CHARACTERS[RANDOM.nextInt(VALID_CHARACTERS.length)];
        }

        return new String(result);
    }
}
