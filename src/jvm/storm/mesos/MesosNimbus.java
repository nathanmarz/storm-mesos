package storm.mesos;

import backtype.storm.scheduler.INimbus;
import backtype.storm.scheduler.IScheduler;
import backtype.storm.scheduler.SupervisorDetails;
import backtype.storm.scheduler.Topologies;
import backtype.storm.scheduler.TopologyDetails;
import backtype.storm.scheduler.WorkerSlot;
import backtype.storm.utils.LocalState;
import com.google.protobuf.ByteString;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.Timer;
import java.util.TimerTask;
import java.util.concurrent.Semaphore;
import org.apache.commons.lang.builder.ToStringBuilder;
import org.apache.log4j.Logger;
import org.apache.mesos.MesosSchedulerDriver;
import org.apache.mesos.Protos.CommandInfo;
import org.apache.mesos.Protos.CommandInfo.URI;
import org.apache.mesos.Protos.ExecutorID;
import org.apache.mesos.Protos.ExecutorInfo;
import org.apache.mesos.Protos.FrameworkID;
import org.apache.mesos.Protos.FrameworkInfo;
import org.apache.mesos.Protos.MasterInfo;
import org.apache.mesos.Protos.Offer;
import org.apache.mesos.Protos.OfferID;
import org.apache.mesos.Protos.Resource;
import org.apache.mesos.Protos.SlaveID;
import org.apache.mesos.Protos.TaskInfo;
import org.apache.mesos.Protos.TaskID;
import org.apache.mesos.Protos.TaskStatus;
import org.apache.mesos.Protos.Value.Range;
import org.apache.mesos.Protos.Value.Ranges;
import org.apache.mesos.Protos.Value.Scalar;
import org.apache.mesos.Protos.Value.Type;
import org.apache.mesos.Scheduler;
import org.apache.mesos.SchedulerDriver;

public class MesosNimbus implements INimbus {
    public static final String CONF_EXECUTOR_URI = "mesos.executor.uri";
    public static final String CONF_MASTER_URL = "mesos.master.url";
    public static final String CONF_MASTER_FAILOVER_TIMEOUT_SECS = "mesos.master.failover.timeout.secs";
    
    public static final Logger LOG = Logger.getLogger(MesosNimbus.class);

    private static final String FRAMEWORK_ID = "FRAMEWORK_ID";
    private final Object OFFERS_LOCK = new Object();
    private RotatingMap<OfferID, Offer> _offers;
    
    LocalState _state;
    NimbusScheduler _scheduler;
    volatile SchedulerDriver _driver;
    Timer _timer = new Timer();

    @Override
    public IScheduler getForcedScheduler() {
        return new MesosNimbusScheduler();
    }

    @Override
    public String getHostName(Map<String, SupervisorDetails> map, String nodeId) {
        return getHostnameFromNodeId(nodeId);
    }

    public class NimbusScheduler implements Scheduler {
        
        Semaphore _initter;
        
        public NimbusScheduler(Semaphore initter) {
            _initter = initter;
        }
        
        @Override
        public void registered(final SchedulerDriver driver, FrameworkID id, MasterInfo masterInfo) {
            _driver = driver;
            try {
                _state.put(FRAMEWORK_ID, id.getValue());
            } catch (IOException e) {
                LOG.error("Halting process...", e);
                Runtime.getRuntime().halt(1);                
            }
            _offers = new RotatingMap<OfferID, Offer>(new RotatingMap.ExpiredCallback<OfferID, Offer>() {
                @Override
                public void expire(OfferID key, Offer val) {
                    driver.declineOffer(key);
                }                
            });
            _timer.scheduleAtFixedRate(new TimerTask() {

                @Override
                public void run() {
                    try {
                        synchronized(OFFERS_LOCK) {
                            _offers.rotate();
                        }
                    } catch(Throwable t) {
                        LOG.error("Received fatal error Halting process...", t);
                        Runtime.getRuntime().halt(2);                        
                    }
                }
                
            }, 0, 20000);
            _initter.release();
        }

        @Override
        public void reregistered(SchedulerDriver sd, MasterInfo info) {
        }

        @Override
        public void disconnected(SchedulerDriver driver) {
        }        

        @Override
        public void resourceOffers(SchedulerDriver driver, List<Offer> offers) {
            synchronized(OFFERS_LOCK) {
                for(Offer offer: offers) {
                    _offers.put(offer.getId(), offer);
                }
            }
        }

        @Override
        public void offerRescinded(SchedulerDriver driver, OfferID id) {
            synchronized(OFFERS_LOCK) {
                _offers.remove(id);
            }
        }

        @Override
        public void statusUpdate(SchedulerDriver driver, TaskStatus status) {
            LOG.info("Received status update: " + status.toString());
        }

        @Override
        public void frameworkMessage(SchedulerDriver driver, ExecutorID executorId, SlaveID slaveId, byte[] data) {
        }

        @Override
        public void slaveLost(SchedulerDriver driver, SlaveID id) {
            LOG.info("Lost slave: " + id.toString());
        }

        @Override
        public void error(SchedulerDriver driver, String msg) {
            LOG.error("Received fatal error \nmsg:" + msg + "\nHalting process...");
            Runtime.getRuntime().halt(2);
        }

        @Override
        public void executorLost(SchedulerDriver driver, ExecutorID executor, SlaveID slave, int status) {
        }
    }
    
    Map _conf;
    
    @Override
    public void prepare(Map conf, String localDir) {
        try {
            _conf = conf;
            _state = new LocalState(localDir);        
            String id = (String) _state.get(FRAMEWORK_ID);


            Semaphore initter = new Semaphore(0);
            _scheduler = new NimbusScheduler(initter);
            Number failoverTimeout = (Number) conf.get(CONF_MASTER_FAILOVER_TIMEOUT_SECS);
            if(failoverTimeout==null) failoverTimeout = 30;
            
            FrameworkInfo.Builder finfo = FrameworkInfo.newBuilder()
                                    .setName("Storm-0.8.0")
                                    .setFailoverTimeout(failoverTimeout.doubleValue())
                                    .setUser("");
            if(id!=null) {
                finfo.setId(FrameworkID.newBuilder().setValue(id).build());
            }
            
            
            MesosSchedulerDriver driver =
                    new MesosSchedulerDriver(
                      _scheduler,
                      finfo.build(),
                      (String) conf.get(CONF_MASTER_URL));
            
            driver.start();
            LOG.info("Waiting for scheduler to initialize...");
            initter.acquire();
            LOG.info("Scheduler initialized...");
        } catch(IOException e) {
            throw new RuntimeException(e);
        } catch(InterruptedException e) {
            throw new RuntimeException(e);            
        }
    }
    
    private static final String NODE_ID_SEPARATOR = "$$$$$";
    
    public static String toNodeId(String hostname, TopologyDetails details) {
        if(hostname.endsWith("$") || hostname.contains(NODE_ID_SEPARATOR)) {
            throw new RuntimeException("Cannot assign node id for hostname " + hostname);
        }
        return hostname + NODE_ID_SEPARATOR + details.getId();
    }
    
    public static String getTopologyIdFromNodeId(String nodeId) {
        int i = nodeId.indexOf(NODE_ID_SEPARATOR);
        return nodeId.substring(i + NODE_ID_SEPARATOR.length());
    }
    
    public static String getHostnameFromNodeId(String nodeId) {
        int i = nodeId.indexOf(NODE_ID_SEPARATOR);
        return nodeId.substring(0, i);
    }
    
    private static class OfferResources {
        int cpuSlots = 0;
        int memSlots = 0;
        List<Integer> ports = new ArrayList<Integer>();

        @Override
        public String toString() {
            return ToStringBuilder.reflectionToString(this);
        }        
    }
    
    private OfferResources getResources(Offer offer, TopologyDetails info) {
        int cpu = MesosCommon.topologyCpu(_conf, info);
        int mem = MesosCommon.topologyMem(_conf, info);
        int workers = MesosCommon.numWorkers(_conf, info);
        
        OfferResources resources = new OfferResources();
        
        for(Resource r: offer.getResourcesList()) {
            if(r.getName().equals("cpus")) {
                resources.cpuSlots = (int) Math.floor(r.getScalar().getValue() / cpu);
            } else if(r.getName().equals("mem")) {
                resources.memSlots = (int) Math.floor(r.getScalar().getValue() / mem);                
            } else if(r.getName().equals("ports")) {
                for(Range range: r.getRanges().getRangeList()) {
                    if(resources.ports.size() >= workers) {
                        break;
                    } else {
                        int start = (int) range.getBegin();
                        int end = (int) range.getEnd();
                        for(int p=start; p<=end; p++) {
                            resources.ports.add(p);
                            if(resources.ports.size()>=workers) {
                                break;
                            }
                        }
                    }
                }
            }
        }
        LOG.debug("Offer: " + offer.toString());
        LOG.debug("Extracted resources: " + resources.toString());
        return resources;
    }

    private List<WorkerSlot> toSlots(Offer offer, TopologyDetails info) {
        OfferResources resources = getResources(offer, info);
        
        List<WorkerSlot> ret = new ArrayList<WorkerSlot>();
        int availableSlots = Math.min(resources.cpuSlots, resources.memSlots);
        availableSlots = Math.min(availableSlots, resources.ports.size());
        for(int i=0; i<availableSlots; i++) {
            ret.add(new WorkerSlot(toNodeId(offer.getHostname(), info), resources.ports.get(i)));
        }
        return ret;
    }
    
    
    Map<String, Long> _firstTopologyTime = new HashMap<String, Long>();
    
    
    @Override
    public Collection<WorkerSlot> availableSlots(Collection<SupervisorDetails> existingSupervisors, Collection<WorkerSlot> usedSlots, Topologies topologies, Collection<String> topologiesMissingAssignments) {
        Set<String> topologiesMissingAssignmentsSet;
        if(topologiesMissingAssignments==null) {
            topologiesMissingAssignmentsSet = new HashSet();
        } else {
            topologiesMissingAssignmentsSet = new HashSet<String>(topologiesMissingAssignments);            
        }
        synchronized(OFFERS_LOCK) {
            LOG.info("Currently have " + _offers.size() + " offers buffered");
            if(!topologiesMissingAssignmentsSet.isEmpty()) {
                LOG.info("Topologies that need assignments: " + topologiesMissingAssignmentsSet.toString());            
            }
        }
        
        Map<String, Integer> numExistingWorkers = new HashMap();
        for(TopologyDetails details: topologies.getTopologies()) {
            numExistingWorkers.put(details.getId(), 0);
        }
        for(WorkerSlot s: usedSlots) {
            String topologyId = getTopologyIdFromNodeId(s.getNodeId());
            numExistingWorkers.put(topologyId, numExistingWorkers.get(topologyId) + 1);
        }
        
        List<TopologyDetails> needed = new ArrayList<TopologyDetails>();
        List<TopologyDetails> filled = new ArrayList<TopologyDetails>();
        for(TopologyDetails t: topologies.getTopologies()) {
            if(topologiesMissingAssignmentsSet.contains(t.getId()) ||
                numExistingWorkers.get(t.getId()) < t.getNumWorkers()) {
                needed.add(t);
            } else {
                filled.add(t);
            }
        }
        Collections.shuffle(needed);
        Collections.shuffle(filled);
        List<TopologyDetails> assignOrder = new ArrayList<TopologyDetails>();
        assignOrder.addAll(needed);
        assignOrder.addAll(filled);

        
        LinkedList<String> desiredTopologySlots = new LinkedList();
        for(TopologyDetails details: assignOrder) {
            for(int i=0; i<details.getNumWorkers(); i++) {
                desiredTopologySlots.add(details.getId());
            }
        }
        
        Map<String, List<WorkerSlot>> potentialSlots = new HashMap();
        for(TopologyDetails details: topologies.getTopologies()) {
            potentialSlots.put(details.getId(), new ArrayList());
        }
        synchronized(OFFERS_LOCK) {
            for(Offer offer: _offers.values()) {
                if(!desiredTopologySlots.isEmpty()) {
                    String nextTopologyId = desiredTopologySlots.getFirst();
                    List<WorkerSlot> availableSlots = toSlots(offer, topologies.getById(nextTopologyId));
                    for(int i=0; i<availableSlots.size(); i++) {
                        potentialSlots.get(nextTopologyId).add(availableSlots.get(i));
                        desiredTopologySlots.removeFirst();
                        if(desiredTopologySlots.isEmpty() || !desiredTopologySlots.getFirst().equals(nextTopologyId)) {
                            break;
                        }
                    }
                }
            }
        }
        
        List<WorkerSlot> retSlots = new ArrayList<WorkerSlot>();
        
        for(String topologyId: potentialSlots.keySet()) {
            List<WorkerSlot> maybeSlots = potentialSlots.get(topologyId);
            TopologyDetails details = topologies.getById(topologyId);
            Long topoStartTime = _firstTopologyTime.get(topologyId);
            if(topoStartTime==null) {
                topoStartTime = System.currentTimeMillis();
                //TODO: mem leak here... need to know when the topology is shut down
                _firstTopologyTime.put(topologyId, topoStartTime);
            }
            long deltaMs = System.currentTimeMillis() - topoStartTime;
            //always makes assignment if topology has been around more than 60 seconds
            if(numExistingWorkers.get(topologyId) > 0 || maybeSlots.size() >= details.getNumWorkers() || deltaMs > (1000 * 60)) {
                retSlots.addAll(maybeSlots);
            }
        }
        LOG.info("Number of available slots: " + retSlots.size());
        return retSlots;
    }

    private OfferID findOffer(WorkerSlot worker, TopologyDetails details) {
        for(Offer offer: _offers.values()) {
            if(toNodeId(offer.getHostname(), details).equals(worker.getNodeId())) {
                OfferResources resources = getResources(offer, details);
                if(resources.ports.contains(worker.getPort())) {
                    return offer.getId();
                }
            }
        }
        return null;
    }
    
    @Override
    public void assignSlots(Topologies topologies, Collection<WorkerSlot> slots) {
        //use ports to determine what slot belongs where
        synchronized(OFFERS_LOCK) {
            Map<OfferID, List<WorkerSlot>> assignment = new HashMap<OfferID, List<WorkerSlot>>();
            for(WorkerSlot slot: slots) {
                OfferID id = findOffer(slot, topologies.getById(getTopologyIdFromNodeId(slot.getNodeId())));
                if(id!=null) {
                    if(!assignment.containsKey(id)) {
                        assignment.put(id, new ArrayList<WorkerSlot>());
                    }
                    assignment.get(id).add(slot);
                }
            }
            
            Map<String, Integer> topologyCpu = new HashMap();
            Map<String, Integer> topologyMem = new HashMap();
            
            
            for(TopologyDetails details: topologies.getTopologies()) {
                topologyCpu.put(details.getId(), MesosCommon.topologyCpu(_conf, details));
                topologyMem.put(details.getId(), MesosCommon.topologyMem(_conf, details));
            }


            for(OfferID id: assignment.keySet()) {
                Offer offer = _offers.get(id);
                List<TaskInfo> tasks = new ArrayList<TaskInfo>();
                for(WorkerSlot slot: assignment.get(id)) {    
                    String topologyId = getTopologyIdFromNodeId(slot.getNodeId());
                    TopologyDetails details = topologies.getById(topologyId);
                    TaskInfo task = TaskInfo.newBuilder()
                            .setName("worker " + slot.getNodeId() + ":" + slot.getPort())
                            .setTaskId(TaskID.newBuilder()
                                .setValue(MesosCommon.taskId(slot.getNodeId(), slot.getPort())))
                            .setSlaveId(offer.getSlaveId())
                            .setExecutor(ExecutorInfo.newBuilder()
                                .setExecutorId(ExecutorID.newBuilder().setValue(details.getId()))
                                .setData(ByteString.copyFromUtf8(slot.getNodeId()))
                                .setCommand(CommandInfo.newBuilder()
                                    .addUris(URI.newBuilder().setValue((String) _conf.get(CONF_EXECUTOR_URI)))
                                    .setValue("cd storm-mesos* && python bin/storm-mesos supervisor")
                            ))
                            .addResources(Resource.newBuilder()
                                .setName("cpus")
                                .setType(Type.SCALAR)
                                .setScalar(Scalar.newBuilder().setValue(topologyCpu.get(topologyId))))
                            .addResources(Resource.newBuilder()
                                .setName("mem")
                                .setType(Type.SCALAR)
                                .setScalar(Scalar.newBuilder().setValue(topologyMem.get(topologyId))))
                            .addResources(Resource.newBuilder()
                                .setName("ports")
                                .setType(Type.RANGES)
                                .setRanges(Ranges.newBuilder()
                                    .addRange(Range.newBuilder()
                                        .setBegin(slot.getPort())
                                        .setEnd(slot.getPort()))))
                            .build();
                    tasks.add(task);
                }
                LOG.info("Launching tasks for offer " + id.getValue() + "\n" + tasks.toString());
                _driver.launchTasks(id, tasks);
                
                _offers.remove(id);                
            }
        }
    }
    
    public static void main(String[] args) {
        backtype.storm.daemon.nimbus.launch(new MesosNimbus());
    }
}
