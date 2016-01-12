package au.edu.ersa.reporting.http.resources;

import io.dropwizard.auth.Auth;
import io.swagger.annotations.Api;
import io.swagger.annotations.ApiParam;

import java.util.function.Function;
import java.util.stream.Collectors;

import javax.ws.rs.Consumes;
import javax.ws.rs.DELETE;
import javax.ws.rs.GET;
import javax.ws.rs.PUT;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.core.MediaType;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import au.edu.ersa.reporting.http.KafkaReporting;
import au.edu.ersa.reporting.http.api.UserList;
import au.edu.ersa.reporting.security.ACL;
import au.edu.ersa.reporting.security.ACL.Permission;
import au.edu.ersa.reporting.security.BasicAuth;
import au.edu.ersa.reporting.security.User;

@Path(KafkaReporting.VERSION_PREFIX + "user")
@Api(value = "user")
@Consumes(MediaType.APPLICATION_JSON)
@Produces(MediaType.APPLICATION_JSON)
public class UserResource extends ReportingResource {
    private final static Logger LOG = LoggerFactory.getLogger(UserResource.class);

    private final BasicAuth auth;

    public UserResource(BasicAuth auth) {
        this.auth = auth;
    }

    private static Sanitise SANITISE = new Sanitise();
    private static class Sanitise implements Function<User,User> {
        @Override
        public User apply(User user) {
            return user.clone(true);
        }
    }

    @PUT
    @Path("/{id}")
    public User add(@Auth @ApiParam("ignore") User user, @PathParam("id") String id) {
        require(user.admin, () -> LOG.warn("not admin: " + user.id));

        try {
            return auth.createUser(id);
        } catch (Exception e) {
            throw CONFLICT;
        }
    }

    @GET
    @Path("/{id}")
    public User get(@Auth @ApiParam("ignore") User user, @PathParam("id") String id) {
        require(user.admin || user.id.equalsIgnoreCase(id), () -> LOG.warn("denied user retrieval: {} on {}", user.id, id));

        LOG.info("user retrieval by {} on {}", user.id, id);

        User retrieved = auth.getUser(id);

        if (retrieved != null) {
            return retrieved.sanitise().stripUserFromACLs();
        } else {
            throw NOT_FOUND;
        }
    }

    @GET
    public UserList list(@Auth @ApiParam("ignore") User user) {
        require(user.admin, () -> LOG.warn("not admin: " + user.id));

        LOG.info("user listing by {}", user.id);

        return new UserList(auth.getAllUsers().stream().map(SANITISE).collect(Collectors.toList()));
    }

    @DELETE
    @Path("/{id}")
    public void delete(@Auth @ApiParam("ignore") User user, @PathParam("id") String id) {
        require(user.admin || user.id.equalsIgnoreCase(id), () -> LOG.warn("denied user deletion: {} on {}", user.id, id));

        if (!auth.deleteUser(id)) {
            throw NOT_FOUND;
        }
    }

    @PUT
    @Path("/{id}/admin")
    public User grantAdmin(@Auth @ApiParam("ignore") User user, @PathParam("id") String id) {
        require(user.admin, () -> LOG.warn("denied granting of administration privilege: {} on {}", user.id, id));

        if (auth.setAdmin(id, true)) {
            return auth.getUser(id).sanitise().stripUserFromACLs();
        } else {
            throw NOT_FOUND;
        }
    }

    @DELETE
    @Path("/{id}/admin")
    public User revokeAdmin(@Auth @ApiParam("ignore") User user, @PathParam("id") String id) {
        require(user.admin, () -> LOG.warn("denied revocation of administration privilege: {} on {}", user.id, id));

        if (auth.setAdmin(id, false)) {
            return auth.getUser(id).sanitise().stripUserFromACLs();
        } else {
            throw NOT_FOUND;
        }
    }

    @DELETE
    @Path("/{id}/token")
    public User resetToken(@Auth @ApiParam("ignore") User user, @PathParam("id") String id) {
        require(user.admin || user.id.equalsIgnoreCase(id), () -> LOG.warn("denied token reset: {} on {}", user.id, id));

        User reset = auth.resetCredentials(id);

        if (reset != null) {
            return reset;
        } else {
            throw NOT_FOUND;
        }
    }

    @PUT
    @Path("/{id}/{topic}/{permission}")
    public User setTopicACL(@Auth @ApiParam("ignore") User user, @PathParam("id") String id, @PathParam("topic") String topic, @PathParam("permission") String permission) {
        require(user.admin, () -> LOG.warn("denied topic ACL addition: user {} topic {} by {}", id, topic, user.id));

        Permission permissionValue = Permission.parse(permission);
        if (permissionValue == null) { throw BAD_REQUEST; }

        User target = auth.getUser(id);

        if (target != null) {
            auth.setACL(id, topic, permissionValue);

            return auth.getUser(id).sanitise().stripUserFromACLs();
        } else {
            throw NOT_FOUND;
        }
    }

    @DELETE
    @Path("/{id}/{topic}")
    public User deleteTopicACL(@Auth @ApiParam("ignore") User user, @PathParam("id") String id, @PathParam("topic") String topic) {
        require(user.admin, () -> LOG.warn("denied topic ACL deletion: user {} topic {} by {}", id, topic, user.id));

        User target = auth.getUser(id);
        ACL acl = auth.getACL(id, topic);

        if ((target != null) && (acl != null)) {
            auth.deleteACL(id, topic);

            return auth.getUser(id).sanitise().stripUserFromACLs();
        } else {
            throw NOT_FOUND;
        }
    }
}
