package storm.mesos;

import backtype.storm.scheduler.Cluster;
import backtype.storm.scheduler.ExecutorDetails;
import backtype.storm.scheduler.IScheduler;
import backtype.storm.scheduler.SchedulerAssignment;
import backtype.storm.scheduler.Topologies;
import backtype.storm.scheduler.TopologyDetails;
import backtype.storm.scheduler.WorkerSlot;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
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
        for(TopologyDetails details: topologies.getTopologies()) {
            List<WorkerSlot> availableSlots = cluster.getAvailableSlots();
            
            
            List<WorkerSlot> myAvailable = new ArrayList<WorkerSlot>();
            for(WorkerSlot s: availableSlots) {
                if(MesosNimbus.getTopologyIdFromNodeId(s.getNodeId()).equals(details.getId())) {
                    myAvailable.add(s);
                }
            }

            if(!myAvailable.isEmpty()) {
                SchedulerAssignment a = cluster.getAssignmentById(details.getId());

                Map<WorkerSlot, List<ExecutorDetails>> slotAllocations = getSlotAllocations(a);
                
                
                int avg = details.getExecutors().size() / details.getNumWorkers();
                
                if(slotAllocations.size() < details.getNumWorkers() && myAvailable.size() > slotAllocations.size()) {
                    for(WorkerSlot s: slotAllocations.keySet()) {
                        //TODO: would be more precise to check the distribution.. can end up with unbalanced topology
                        // this way if it shrinks and then grows to numWorkers
                        int numE = slotAllocations.get(s).size();
                        if(numE != avg && numE != avg + 1) {
                            cluster.freeSlot(s);
                        }

                        // can't do this because it may not be a recognized slot by mesos anymore
                        // myAvailable.add(s);
                    }
                }
                    //keep at most numWorkers
                while(myAvailable.size() > details.getNumWorkers()) {
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
