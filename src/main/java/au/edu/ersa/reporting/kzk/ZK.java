package au.edu.ersa.reporting.kzk;

import io.dropwizard.lifecycle.Managed;

import java.io.IOException;
import java.util.Collections;
import java.util.List;
import java.util.Map;

import org.apache.curator.CuratorZookeeperClient;
import org.apache.curator.RetryLoop;
import org.apache.curator.framework.CuratorFramework;
import org.apache.zookeeper.ZooKeeper.States;
import org.apache.zookeeper.data.Stat;

import au.edu.ersa.reporting.http.Util;
import au.edu.ersa.reporting.http.Wrap;

import com.codahale.metrics.health.HealthCheck.Result;

public class ZK implements Managed {
    public final CuratorFramework curator;

    public ZK(CuratorFramework curator) {
        this.curator = curator;
    }

    @Override
    public void start() throws Exception {
        curator.start();
    }

    @Override
    public void stop() throws Exception {
        curator.close();
    }

    public void delete(String path) {
        Wrap.runtimeException(() -> curator.delete().guaranteed().deletingChildrenIfNeeded().forPath(path));
    }

    public List<String> list(String path) {
        if (!exists(path)) {
            return Collections.emptyList();
        } else {
            return Wrap.runtimeException(() -> curator.getChildren().forPath(path));
        }
    }

    public Stat stat(String path) {
        return Wrap.runtimeException(() -> curator.checkExists().forPath(path));
    }

    public boolean exists(String path) {
        return stat(path) != null;
    }

    public String get(String path) {
        if (!exists(path)) { return null; }

        byte[] data = Wrap.runtimeException(() -> curator.getData().forPath(path));
        if (data != null) {
            return new String(data);
        } else {
            return null;
        }
    }

    public Node walk(String path) { return walk(path, true); }

    private static Map<?,?> parseMap(String s) {
        try {
            return Util.JSON.readValue(s, Map.class);
        } catch (IOException e) { return null; }
    }

    private Node walk(String path, boolean isRoot) {
        Node node = new Node(path.substring(path.lastIndexOf('/') + 1), isRoot);

        List<String> list = list(path);
        if (!list.isEmpty()) {
            for (String child : list(path)) {
                node.addChild(walk(path + "/" + child, false));
            }
        }

        String data = get(path);
        if (data != null) {
            if (data.startsWith("{")) {
                Map<?,?> subMap = parseMap(data);

                if (subMap != null) {
                    for (Map.Entry<?,?> entry : subMap.entrySet()) {
                        String key = (String)entry.getKey();
                        if (node.containsKey(key)) {
                            key = "data/" + key;
                        }
                        node.put(key, entry.getValue());
                    }
                } else {
                    node.setData(data);
                }
            } else {
                node.setData(data);
            }
        }

        return node;
    }

    public Result getState() {
        try {
            CuratorZookeeperClient client = curator.getZookeeperClient();
            States state = RetryLoop.callWithRetry(client, () -> client.getZooKeeper().getState());
            return Result.healthy(state.toString());
        } catch (Exception e) {
            return Result.unhealthy(e);
        }
    }
}

