package au.edu.ersa.reporting.http.api;

import java.util.HashMap;
import java.util.Map;

public class Topic {
    public String name;

    public Data summary;
    public Map<String,State> partition = new HashMap<>();

    public Topic() {}

    public Topic(String name) {
        this.name = name;
    }

    public static class Data {
        // {"version":1,"partitions":{"2":[1,2,0],"1":[0,1,2],"0":[2,0,1]}}

        public int version;
        public Map<String,int[]> partitions;
    }

    public static class State {
        // {"controller_epoch":7,"leader":1,"version":1,"leader_epoch":7,"isr":[1,0,2]}

        public int controllerEpoch, leader, version, leaderEpoch;
        public int[] isr;

        public long earliestOffset, latestOffset;
    }
}
