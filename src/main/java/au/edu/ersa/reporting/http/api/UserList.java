package au.edu.ersa.reporting.http.api;

import java.util.Collections;
import java.util.List;

import au.edu.ersa.reporting.security.User;

public class UserList extends ReportingResponse {
    public final List<User> users;

    public UserList(List<User> list) {
        if (list != null) {
            this.users = Collections.unmodifiableList(list);
        } else {
            this.users = Collections.emptyList();
        }
    }
}
