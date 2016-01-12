package au.edu.ersa.reporting.kzk;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

import com.fasterxml.jackson.annotation.JsonIgnore;

@SuppressWarnings("serial")
public class Node extends HashMap<String,Object> {
    @JsonIgnore
    public final String name;

    public Node(String name) {
        this(name, false);
    }

    public Node(String name, boolean includeName) {
        this.name = name;
        if (includeName) { put("name", name); }
    }

    public void setData(String data) {
        put("data", data);
    }

    public String getData() {
        return (String)get("data");
    }

    public void addChild(Node node) {
        put(node.name, node);
    }

    public List<Node> getChildren() {
        List<Node> list = new ArrayList<>();
        for (Entry<String,Object> entry : entrySet()) {
            if ((!entry.getKey().equals("name")) && (!entry.getKey().equals("data"))) {
                list.add((Node)entry.getValue());
            }
        }
        return list;
    }
}