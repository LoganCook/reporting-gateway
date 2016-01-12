package au.edu.ersa.reporting.security;

import au.edu.ersa.reporting.http.Util;

import com.amazonaws.services.dynamodbv2.datamodeling.DynamoDBAttribute;
import com.amazonaws.services.dynamodbv2.datamodeling.DynamoDBHashKey;
import com.amazonaws.services.dynamodbv2.datamodeling.DynamoDBRangeKey;
import com.amazonaws.services.dynamodbv2.datamodeling.DynamoDBTable;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonInclude.Include;

@DynamoDBTable(tableName = "reporting-acl")
@JsonInclude(Include.NON_NULL)
public class ACL implements Cloneable {
    public static enum Permission {
        READ_ONLY("r"), READ_WRITE("rw"), WRITE_ONLY("w");

        private String value;

        private Permission(String value) {
            this.value = value;
        }

        @Override
        public String toString() { return value.toLowerCase(); }

        public boolean allows(Permission permission) {
            return toString().contains(permission.toString());
        }

        public static Permission parse(String s) {
            for (Permission permissionValue : values()) {
                if (permissionValue.value.equalsIgnoreCase(s)) {
                    return permissionValue;
                }
            }

            return null;
        }
    }

    private String id, topic, permission;

    @DynamoDBHashKey
    public String getId() {
        return id;
    }

    public void setId(String id) {
        this.id = id;
    }

    @DynamoDBRangeKey
    public String getTopic() {
        return topic;
    }

    public void setTopic(String topic) {
        this.topic = topic;
    }

    @DynamoDBAttribute
    public String getPermission() {
        return permission;
    }

    public void setPermission(Permission permission) {
        this.permission = permission.toString();
    }

    public ACL() {}

    public ACL(String id, String topic, Permission permission) {
        this(id, topic);
        this.permission = permission.toString();
    }

    public ACL(String id, String topic) {
        this.id = id;
        this.topic = topic;
    }

    @Override
    public String toString() {
        return Util.toJSON(this);
    }

    @Override
    public ACL clone() { return new ACL(id, topic, Permission.parse(permission)); }
}
