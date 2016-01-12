package au.edu.ersa.reporting.security;

import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;

import au.edu.ersa.reporting.http.Util;

import com.amazonaws.services.dynamodbv2.datamodeling.DynamoDBAttribute;
import com.amazonaws.services.dynamodbv2.datamodeling.DynamoDBHashKey;
import com.amazonaws.services.dynamodbv2.datamodeling.DynamoDBIgnore;
import com.amazonaws.services.dynamodbv2.datamodeling.DynamoDBTable;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonInclude.Include;

@DynamoDBTable(tableName = "reporting-auth")
@JsonInclude(Include.NON_NULL)
public class User implements Cloneable {
    public String id, secret;

    public boolean admin = false;

    public List<ACL> access;

    @DynamoDBHashKey
    public String getId() {
        return id;
    }

    public void setId(String id) {
        this.id = id;
    }

    @DynamoDBAttribute
    public String getSecret() {
        return secret;
    }

    public void setSecret(String secret) {
        this.secret = secret;
    }

    @DynamoDBAttribute
    public boolean isAdmin() {
        return admin;
    }

    public void setAdmin(boolean admin) {
        this.admin = admin;
    }

    @DynamoDBIgnore
    public List<ACL> getAccess() {
        return access;
    }

    public void setAccess(List<ACL> access) {
        this.access = Collections.unmodifiableList(access);
    }

    public User() {}

    public User(String id) {
        this(id, false);
    }

    public User(String id, boolean admin) {
        this(id, null, admin);
    }

    public User(String id, String secret) {
        this(id, secret, false);
    }

    public User(String id, String secret, boolean admin) {
        this.id = id;
        this.secret = secret;
        this.admin = admin;
    }

    private boolean hasACL(String topic, String action) {
        if (access == null) { return false; }

        action = action.toLowerCase();

        for (ACL acl : access) {
            if (acl.getTopic().equalsIgnoreCase(topic) && acl.getPermission().toLowerCase().contains(action)) {
                return true;
            }
        }

        return false;
    }

    public boolean canRead(String topic) {
        return admin || hasACL(topic, "r");
    }

    public boolean canWrite(String topic) {
        return admin || hasACL(topic, "w");
    }

    public User clone(boolean sanitise) {
        User clone = new User(id, sanitise ? null : secret, admin);
        if (access != null) {
            clone.access = access.stream().map(acl -> acl.clone()).collect(Collectors.toList());
        }
        return clone;
    }

    @Override
    public User clone() {
        return clone(false);
    }

    public User sanitise() { return clone(true); }

    public User stripUserFromACLs() {
        if (access != null) {
            for (ACL acl : access) {
                acl.setId(null);
            }
        }
        return this;
    }

    @Override
    public String toString() {
        return Util.toJSON(this);
    }
}
