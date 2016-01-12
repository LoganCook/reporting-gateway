package au.edu.ersa.reporting.http.resources;

import javax.ws.rs.WebApplicationException;

import org.eclipse.jetty.http.HttpStatus;

public class ReportingResource {
    protected static final WebApplicationException FORBIDDEN = new WebApplicationException(HttpStatus.FORBIDDEN_403);
    protected static final WebApplicationException INTERNAL_SERVER_ERROR = new WebApplicationException(HttpStatus.INTERNAL_SERVER_ERROR_500);
    protected static final WebApplicationException NOT_FOUND = new WebApplicationException(HttpStatus.NOT_FOUND_404);
    protected static final WebApplicationException BAD_REQUEST = new WebApplicationException(HttpStatus.BAD_REQUEST_400);
    protected static final WebApplicationException CONFLICT = new WebApplicationException(HttpStatus.CONFLICT_409);

    protected static WebApplicationException BAD_REQUEST(String s) {
        return new WebApplicationException(s, HttpStatus.BAD_REQUEST_400);
    }

    protected static WebApplicationException INTERNAL_SERVER_ERROR(String s) {
        return new WebApplicationException(s, HttpStatus.INTERNAL_SERVER_ERROR_500);
    }

    protected static void require(boolean requirement) { require(requirement, null); }

    protected static void require(boolean requirement, Runnable onFail) {
        if (!requirement) {
            if (onFail != null) { onFail.run(); }
            throw FORBIDDEN;
        }
    }
}
