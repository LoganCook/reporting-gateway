package au.edu.ersa.reporting.http.health;

import au.edu.ersa.reporting.kzk.ZK;

import com.codahale.metrics.health.HealthCheck;

public class ZKHealthCheck extends HealthCheck {
    private final ZK zk;

    public ZKHealthCheck(ZK zk) {
        this.zk = zk;
    }

    @Override
    protected Result check() {
        return zk.getState();
    }
}
