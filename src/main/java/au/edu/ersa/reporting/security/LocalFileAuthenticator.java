package au.edu.ersa.reporting.security;

import io.dropwizard.auth.AuthenticationException;
import io.dropwizard.auth.basic.BasicCredentials;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.attribute.PosixFileAttributes;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import au.edu.ersa.reporting.security.ACL.Permission;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory;
import com.google.common.base.Optional;
import com.google.common.util.concurrent.Uninterruptibles;

public class LocalFileAuthenticator extends BasicAuth {
    private final static Logger LOG = LoggerFactory.getLogger(LocalFileAuthenticator.class);
    private final static ObjectMapper YAML = new ObjectMapper(new YAMLFactory());

    private final Refresh refresh;

    private Map<String,User> local = Collections.emptyMap();

    public LocalFileAuthenticator(File file, AuthAlgorithm auth) {
        super(auth);

        refresh = new Refresh(file, (map) -> { local = map; });
    }

    private static class Refresh implements Runnable {
        private final Consumer<Map<String,User>> update;
        private final File file;
        private boolean running = true;
        private long lastModified = 0;
        private int failures = 0;

        public Refresh(File file, Consumer<Map<String,User>> update) {
            this.file = file;
            this.update = update;
        }

        public void start() {
            new Thread(this, toString()).start();
        }

        public void stop() {
            running = false;
        }

        private void sleep() {
            int seconds = 2 * (int)Math.pow(2, failures);
            LOG.debug("Sleeping for {} seconds", seconds);
            Uninterruptibles.sleepUninterruptibly(seconds, TimeUnit.SECONDS);
        }

        @Override
        public void run() {
            while (running) {
                try {
                    long lastModifiedCheck = Files.readAttributes(file.toPath(), PosixFileAttributes.class).lastModifiedTime().toMillis();
                    if (lastModifiedCheck > lastModified) {
                        LOG.info("Reloading: " + file);
                        update.accept(Collections.unmodifiableMap(load(file)));
                        lastModified = lastModifiedCheck;
                        failures = 0;
                    }
                } catch (Exception e) {
                    LOG.error("Error refreshing: " + file, e);
                    failures++;
                }

                sleep();
            }
        }

        private static class AuthFile {
            public List<User> user;
            public List<ACL> acl;
        }

        private Map<String,User> load(File file) throws IOException {
            Map<String,User> map = new HashMap<>();

            AuthFile authFile = YAML.readValue(file, AuthFile.class);

            for (User user : authFile.user) {
                map.put(user.id, user);
            }

            for (ACL acl : authFile.acl) {
                User entry = map.get(acl.getId());
                if (entry != null) {

                    entry.access.add(acl);
                }
            }

            return Collections.unmodifiableMap(map);
        }
    }

    @Override
    public Optional<User> authenticate(BasicCredentials credentials) throws AuthenticationException {
        User user = local.get(credentials.getUsername());

        if ((user != null) && auth.isValid(credentials, user)) {
            LOG.info("authenticated: " + user.id);
            return Optional.of(user);
        } else {
            LOG.warn("authentication failed: " + credentials.getUsername());
            return Optional.absent();
        }
    }

    @Override
    public void start() throws Exception {
        refresh.start();
    }

    @Override
    public void stop() throws Exception {
        refresh.stop();
    }

    @Override
    public User getUser(String id) {
        return local.get(id);
    }

    @Override
    public boolean canPerform(String id, String topic, Permission permission) {
        User userACL = local.get(id);

        if (userACL == null) { return false; }

        for (ACL acl : userACL.access) {
            if (acl.getTopic().equalsIgnoreCase(topic) && acl.getPermission().contains(permission.toString())) {
                return true;
            }
        }

        return false;
    }

    @Override
    public List<User> getAllUsers() {
        return Collections.unmodifiableList(new ArrayList<>(local.values()));
    }

    @Override
    public int getUserCount() {
        return local.size();
    }

    @Override
    public ACL getACL(String id, String topic) {
        User user = local.get(id);

        if (user != null) {
            for (ACL acl : user.access) {
                if (acl.getTopic().equalsIgnoreCase(topic)) {
                    return acl;
                }
            }
        }

        return null;
    }
}
