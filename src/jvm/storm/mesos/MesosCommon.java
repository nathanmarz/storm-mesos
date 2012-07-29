package storm.mesos;

import backtype.storm.Config;
import backtype.storm.scheduler.TopologyDetails;
import java.util.HashMap;
import java.util.Map;
import org.apache.log4j.Logger;

public class MesosCommon {
    public static final Logger LOG = Logger.getLogger(MesosCommon.class);
    
    public static final String CPU_CONF = "topology.mesos.worker.cpu";
    public static final String MEM_CONF = "topology.mesos.worker.mem.mb";
    public static final String SUICIDE_CONF = "mesos.supervisor.suicide.inactive.timeout.secs";
    
    public static final int DEFAULT_CPU = 1;
    public static final int DEFAULT_MEM_MB = 1024;
    public static final int DEFAULT_SUICIDE_TIMEOUT_SECS = 60;
    
    public static String taskId(String nodeid, int port) {
        return nodeid + "-" + port;
    }
    
    public static int portFromTaskId(String taskId) {
        int last = taskId.lastIndexOf("-");
        String port = taskId.substring(last+1);
        return Integer.parseInt(port);
    }
    
    public static int getSuicideTimeout(Map conf) {
        Number timeout = (Number) conf.get(SUICIDE_CONF);
        if(timeout==null) return DEFAULT_SUICIDE_TIMEOUT_SECS;
        else return timeout.intValue();        
    }
    
    public static Map getFullTopologyConfig(Map conf, TopologyDetails info) {
        Map ret = new HashMap(conf);
        ret.putAll(info.getConf());
        return ret;        
    }
    
    public static int topologyCpu(Map conf, TopologyDetails info) {
        conf = getFullTopologyConfig(conf, info);
        Object cpuObj = conf.get(CPU_CONF);
        if(cpuObj!=null && !(cpuObj instanceof Number)) {
            LOG.warn("Topology has invalid mesos cpu configuration: " + cpuObj + " for topology " + info.getId());
            cpuObj = null;
        }
        if(cpuObj==null) return DEFAULT_CPU;
        else return ((Number)cpuObj).intValue();
    }

    public static int topologyMem(Map conf, TopologyDetails info) {
        conf = getFullTopologyConfig(conf, info);
        Object memObj = conf.get(MEM_CONF);
        if(memObj!=null && !(memObj instanceof Number)) {
            LOG.warn("Topology has invalid mesos mem configuration: " + memObj + " for topology " + info.getId());
            memObj = null;
        }
        if(memObj==null) return DEFAULT_MEM_MB;
        else return ((Number)memObj).intValue();
    }
    
    public static int numWorkers(Map conf, TopologyDetails info) {
        conf = getFullTopologyConfig(conf, info);
        return((Number) conf.get(Config.TOPOLOGY_WORKERS)).intValue();
    }
}
