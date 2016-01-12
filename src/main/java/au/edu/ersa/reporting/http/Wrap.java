package au.edu.ersa.reporting.http;

import java.util.concurrent.Callable;

import javax.ws.rs.WebApplicationException;

public class Wrap {
    public static <T> T webApplicationException(Callable<T> callable) {
        try {
            return callable.call();
        } catch (Exception e) { throw new WebApplicationException(e); }
    }

    public static void webApplicationException(Runnable runnable) {
        try {
            runnable.run();
        } catch (Exception e) { throw new WebApplicationException(e); }
    }

    public static <T> T runtimeException(Callable<T> callable) {
        try {
            return callable.call();
        } catch (Exception e) { throw new RuntimeException(e); }
    }

    public static void runtimeException(Runnable runnable) {
        try {
            runnable.run();
        } catch (Exception e) { throw new RuntimeException(e); }
    }
}
