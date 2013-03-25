package storm.mesos;

import backtype.storm.scheduler.TopologyDetails;
import clojure.lang.IFn;

import java.util.Map;

public interface IMesosNimbusStateHelper {
    void init(Map conf);
    String getExecutorURI(TopologyDetails details);
}
