package storm.mesos;

import backtype.storm.scheduler.Cluster;
import backtype.storm.scheduler.ExecutorDetails;
import backtype.storm.scheduler.IScheduler;
import backtype.storm.scheduler.SchedulerAssignment;
import backtype.storm.scheduler.Topologies;
import backtype.storm.scheduler.TopologyDetails;
import backtype.storm.scheduler.WorkerSlot;
import backtype.storm.utils.Utils;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.log4j.Logger;

public class MesosNimbusScheduler implements IScheduler {
    public static final Logger LOG = Logger.getLogger(MesosNimbusScheduler.class);

    
    private Map<WorkerSlot, List<ExecutorDetails>> getSlotAllocations(SchedulerAssignment a) {
        if(a==null) return new HashMap();
        Map<ExecutorDetails, WorkerSlot> em =  a.getExecutorToSlot();
        Map<WorkerSlot, List<ExecutorDetails>> ret = new HashMap();
        for(ExecutorDetails e: em.keySet()) {
            WorkerSlot s = em.get(e);
            if(!ret.containsKey(s)) ret.put(s, new ArrayList());
            ret.get(s).add(e);
        }
        return ret;
    }
    
    @Override
    public void schedule(Topologies topologies, Cluster cluster) {        
        LOG.info("Scheduling for " + MesosCommon.getTopologyIds(topologies.getTopologies()).toString());
        for(TopologyDetails details: topologies.getTopologies()) {
            List<WorkerSlot> availableSlots = cluster.getAvailableSlots();
            
            
            List<WorkerSlot> myAvailable = new ArrayList<WorkerSlot>();
            for(WorkerSlot s: availableSlots) {
                if(MesosNimbus.getTopologyIdFromNodeId(s.getNodeId()).equals(details.getId())) {
                    myAvailable.add(s);
                }
            }

            SchedulerAssignment a = cluster.getAssignmentById(details.getId());

            Map<WorkerSlot, List<ExecutorDetails>> slotAllocations = getSlotAllocations(a);

            if(myAvailable.size() < details.getNumWorkers() && slotAllocations.size() < details.getNumWorkers()) {
                LOG.info("Not enough slots available to fully assign " + details.getId());
                LOG.info("Available slots for " + details.getId() + ": " + myAvailable.toString());
                LOG.info("Curr slots for " + details.getId() + ": " + slotAllocations.toString());
            }

            if(!myAvailable.isEmpty()) {
                
                
                Map<Integer, Integer> distribution = new HashMap(Utils.integerDivided(details.getExecutors().size(), details.getNumWorkers()));                
                
                int keptWorkers = slotAllocations.size();
                if(slotAllocations.size() != details.getNumWorkers() &&
                        (myAvailable.size() > slotAllocations.size()
                         || myAvailable.size() >= details.getNumWorkers())) {
                    for(WorkerSlot s: slotAllocations.keySet()) {
                        int numE = slotAllocations.get(s).size();
                        Integer amt = distribution.get(numE);
                        if(amt!=null && amt > 0) {
                            distribution.put(numE, amt - 1);
                        } else {
                            cluster.freeSlot(s);
                            keptWorkers--;                            
                        }

                        // can't do this because it may not be a recognized slot by mesos anymore
                        // myAvailable.add(s);
                    }
                }
                
                LOG.debug("Topology " + details.getId() + " scheduling summary: \n" +
                           "Existing workers: " + slotAllocations.size() + "\n" + 
                           "Available slots: " + myAvailable.size() + "\n" +
                           "Kept workers: " + keptWorkers + "\n" +
                           "Configured workers: " + details.getNumWorkers() + "\n");
                
                
                //keep at most numWorkers
                while(myAvailable.size() > Math.max(1, details.getNumWorkers() - keptWorkers)) {
                    myAvailable.remove(myAvailable.size()-1);
                }
                Map<WorkerSlot, List<ExecutorDetails>> toAssign = new HashMap();
                for(WorkerSlot s: myAvailable) {
                    toAssign.put(s, new ArrayList());
                }
                int i=0;
                Map<String, List<ExecutorDetails>> toSchedule = cluster.getNeedsSchedulingComponentToExecutors(details);

                for(String c: toSchedule.keySet()) {
                    List<ExecutorDetails> executors = toSchedule.get(c);
                    for(ExecutorDetails e: executors) {
                        WorkerSlot s = myAvailable.get(i);
                        toAssign.get(s).add(e);
                        i = (i + 1) % myAvailable.size();
                    }
                }
                for(WorkerSlot s: toAssign.keySet()) {
                    List<ExecutorDetails> executors = toAssign.get(s);
                    if(executors.size()>0) {
                        cluster.assign(s, details.getId(), executors);                        
                    }
                }
            }
        }
    }
    
}
