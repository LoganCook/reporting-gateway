package au.edu.ersa.reporting.security;

import io.dropwizard.auth.AuthenticationException;
import io.dropwizard.auth.basic.BasicCredentials;

import java.util.ArrayList;
import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import au.edu.ersa.reporting.security.ACL.Permission;

import com.amazonaws.auth.AWSCredentials;
import com.amazonaws.regions.Region;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDB;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDBClient;
import com.amazonaws.services.dynamodbv2.datamodeling.DynamoDBMapper;
import com.amazonaws.services.dynamodbv2.datamodeling.DynamoDBMapperConfig;
import com.amazonaws.services.dynamodbv2.datamodeling.DynamoDBMapperConfig.ConsistentReads;
import com.amazonaws.services.dynamodbv2.datamodeling.DynamoDBMapperConfig.SaveBehavior;
import com.amazonaws.services.dynamodbv2.datamodeling.DynamoDBMapperConfig.TableNameOverride;
import com.amazonaws.services.dynamodbv2.datamodeling.DynamoDBQueryExpression;
import com.amazonaws.services.dynamodbv2.datamodeling.DynamoDBSaveExpression;
import com.amazonaws.services.dynamodbv2.datamodeling.DynamoDBScanExpression;
import com.amazonaws.services.dynamodbv2.model.ExpectedAttributeValue;
import com.google.common.base.Optional;

public class DynamoDBAuthenticator extends BasicAuth {
    private final static Logger LOG = LoggerFactory.getLogger(DynamoDBAuthenticator.class);

    private final AmazonDynamoDB db;
    private final DynamoDBMapper mapper;

    public DynamoDBAuthenticator(AuthAlgorithm auth, AWSCredentials credentials, Region region, String tablePrefix) {
        super(auth);

        db = new AmazonDynamoDBClient(credentials);
        db.setRegion(region);

        DynamoDBMapperConfig config = new DynamoDBMapperConfig.Builder().
                withConsistentReads(ConsistentReads.EVENTUAL).
                withSaveBehavior(SaveBehavior.UPDATE_SKIP_NULL_ATTRIBUTES).
                withTableNameOverride(TableNameOverride.withTableNamePrefix(tablePrefix)).
                build();

        mapper = new DynamoDBMapper(db, config);

        if (getUserCount() == 0) {
            User user = createUser("system", true);
            LOG.info("initial account (please modify or delete!): " + user);
        }
    }

    @Override
    public Optional<User> authenticate(BasicCredentials credentials) throws AuthenticationException {
        User user = getUser(credentials.getUsername());

        if ((user != null) && auth.isValid(credentials, user)) {
            LOG.info("authenticated: " + user.id);
            return Optional.of(user);
        } else {
            LOG.warn("authentication failed: " + credentials.getUsername());
            return Optional.absent();
        }
    }

    @Override
    public User updateUser(User user) {
        user.secret = null;

        mapper.save(user);

        return user;
    }

    @Override
    public boolean setAdmin(String id, boolean isAdmin) {
        User user = getUser(id);

        if (user != null) {
            user.admin = isAdmin;
            mapper.save(user);
            return true;
        } else {
            return false;
        }
    }

    private static final DynamoDBSaveExpression IF_NOT_EXISTS = new DynamoDBSaveExpression().withExpectedEntry("id", new ExpectedAttributeValue(false));

    private User createUser(String id, boolean admin) {
        String secret = generateRandomString();
        User user = new User(id, auth.generateSecret(secret), admin);
        mapper.save(user, IF_NOT_EXISTS);
        user.secret = secret;
        return user;
    }

    @Override
    public User createUser(String id) {
        return createUser(id, false);
    }

    @Override
    public User resetCredentials(String id) {
        User user = getUser(id);

        if (user == null) {
            return null;
        } else {
            String secret = generateRandomString();
            user.secret = auth.generateSecret(secret);
            mapper.save(user);
            user.secret = secret;
            return user;
        }
    }

    @Override
    public User getUser(String id) {
        User user = mapper.load(User.class, id);

        if (user != null) {
            ACL template = new ACL(id, null);
            DynamoDBQueryExpression<ACL> query = new DynamoDBQueryExpression<ACL>().withHashKeyValues(template);

            user.setAccess(new ArrayList<>(mapper.query(ACL.class, query)));
        }

        return user;
    }

    @Override
    public boolean deleteUser(String id) {
        User user = getUser(id);
        if (user != null) {
            mapper.delete(user);
            return true;
        } else {
            return false;
        }
    }

    @Override
    public List<User> getAllUsers() {
        return mapper.scan(User.class, new DynamoDBScanExpression());
    }

    @Override
    public int getUserCount() {
        return mapper.count(User.class, new DynamoDBScanExpression());
    }

    @Override
    public void deleteACL(String id, String topic) {
        mapper.delete(new ACL(id, topic));
    }

    @Override
    public void setACL(String id, String topic, Permission permission) {
        mapper.save(new ACL(id, topic, permission));
    }

    @Override
    public ACL getACL(String id, String topic) {
        return mapper.load(ACL.class, id, topic);
    }
}
