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

public class MesosNimbusScheduler implements IScheduler {

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

                Set<WorkerSlot> usedSlots;
                if(a==null) usedSlots = new HashSet<WorkerSlot>();
                else usedSlots = new HashSet<WorkerSlot>(a.getExecutorToSlot().values()); 
                if(usedSlots.size() < details.getNumWorkers() && myAvailable.size() > usedSlots.size()) {
                    for(WorkerSlot s: usedSlots) {
                        cluster.freeSlot(s);

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
