/**
 * Copyright 2012-2013 University Of Southern California
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */
package org.workflowsim;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

import org.cloudbus.cloudsim.Log;
import org.cloudbus.cloudsim.core.CloudSimTags;
import org.cloudbus.cloudsim.core.SimEntity;
import org.cloudbus.cloudsim.core.SimEvent;
import org.workflowsim.clustering.BasicClustering;
import org.workflowsim.planning.BasePlanningAlgorithm;
import org.workflowsim.planning.DHEFTPlanningAlgorithm;
import org.workflowsim.planning.HEFTPlanningAlgorithm;
import org.workflowsim.planning.RandomPlanningAlgorithm;

import de.huberlin.wbi.dcs.examples.Parameters;
import de.huberlin.wbi.dcs.examples.Parameters.PlanningAlgorithm;
import de.huberlin.wbi.dcs.workflow.Task;
import wtt.info.WorkflowInfo;


/**
 * WorkflowPlanner supports dynamic planning. In the future we will have global
 * and static algorithm here. The WorkflowSim starts from WorkflowPlanner. It
 * picks up a planning algorithm based on the configuration
 *
 * @author Weiwei Chen
 * @since WorkflowSim Toolkit 1.0
 * @date Apr 9, 2013
 *
 */
public final class WorkflowPlanner extends SimEntity {

    /**
     * The task list.
     */
    protected List<Task> taskList;
    
    public Map<Integer, List<Task>> taskListOfWorkflow;
    /**
     * The workflow parser.
     */
    protected WorkflowParser parser;
    /**
     * The associated clustering engine.
     */
    private int clusteringEngineId;
    private ClusteringEngine clusteringEngine;

    /**
     * Created a new WorkflowPlanner object.
     *
     * @param name name to be associated with this entity (as required by
     * Sim_entity class from simjava package)
     * @throws Exception the exception
     * @pre name != null
     * @post $none
     */
    public WorkflowPlanner(String name) throws Exception {
        this(name, 1);
    }

    public WorkflowPlanner(String name, int schedulers) throws Exception {
        super(name);

        setTaskList(new ArrayList<>());
        this.clusteringEngine = new ClusteringEngine(name + "_Merger_", schedulers);
        this.clusteringEngineId = this.clusteringEngine.getId();
        this.parser = new WorkflowParser(getClusteringEngine().getWorkflowEngine().getSchedulerId(0));
        this.taskListOfWorkflow = new HashMap<>();
    }

    /**
     * Gets the clustering engine id
     *
     * @return clustering engine id
     */
    public int getClusteringEngineId() {
        return this.clusteringEngineId;
    }

    /**
     * Gets the clustering engine
     *
     * @return the clustering engine
     */
    public ClusteringEngine getClusteringEngine() {
        return this.clusteringEngine;
    }

    /**
     * Gets the workflow parser
     *
     * @return the workflow parser
     */
    public WorkflowParser getWorkflowParser() {
        return this.parser;
    }

    /**
     * Gets the workflow engine id
     *
     * @return the workflow engine id
     */
    public int getWorkflowEngineId() {
        return getClusteringEngine().getWorkflowEngineId();
    }

    /**
     * Gets the workflow engine
     *
     * @return the workflow engine
     */
    public WorkflowEngine getWorkflowEngine() {
        return getClusteringEngine().getWorkflowEngine();
    }

    /**
     * Processes events available for this Broker.
     *
     * @param ev a SimEvent object
     * @pre ev != null
     * @post $none
     */
    @Override
    public void processEvent(SimEvent ev) {
        switch (ev.getTag()) {
            case WorkflowSimTags.START_SIMULATION:
            	
            	//generate the event send to workflowPlanner
            	for(double time:Parameters.workflowArrival.keySet()) {
//            		List<String> daxList = Parameters.workflowArrival.get(time);
            		send(getId(), time, CloudSimTags.WORKFLOW_ARRIVAL,Parameters.workflowArrival.get(time));
            	}
                break;
            case CloudSimTags.WORKFLOW_ARRIVAL:
            	
            	WorkflowInfo workflowInfo = (WorkflowInfo)ev.getData();
            	List<String> daxList = workflowInfo.workflowFileName;
            	if(daxList.size() == 1) {
            		getWorkflowParser().daxPath = daxList.get(0);
            	}else {
            		getWorkflowParser().daxPath = null;
            		getWorkflowParser().daxPaths = daxList;
            	}
            	getWorkflowParser().parse(workflowInfo.submittedDCindex);
                //setTaskList(getWorkflowParser().getTaskList());
                this.taskListOfWorkflow = getWorkflowParser().taskListOfWorkflow;
                // current is invalid if change to another planning method something will be changed
                processPlanning();
                //processImpactFactors(getTaskList());
                sendNow(getClusteringEngineId(), WorkflowSimTags.JOB_SUBMIT, taskListOfWorkflow);
                //getWorkflowParser().taskListOfWorkflow.clear();
            	break;
            case CloudSimTags.RUN_INITIAL:
            	initialRun();
            	break;
            case CloudSimTags.END_OF_SIMULATION:
                shutdownEntity();
                break;
            // other unknown tags are processed by this method
            default:
                processOtherEvent(ev);
                break;
        }
    }

    private void initialRun() {
		Log.printLine("Simulations: copyStrategy" + Parameters.copystrategy + " runIndex" + Parameters.runIndex);
		this.taskListOfWorkflow = new HashMap<>();
		// initial workflowParse
		getWorkflowParser().workflowId = 0;
		getWorkflowParser().jobIdStartsFrom = 1;
		getWorkflowParser().taskListOfWorkflow = new HashMap<>();
		
		// initial ClusterEngine
		BasicClustering.idIndex = 0;
		File file = new File("./dynamiccloudsim/model/modelInfo-jobinfo.txt");
		try {
			if(!Parameters.isExtracte) {
				ClusteringEngine.out = new FileWriter(file);
			}else {
				ClusteringEngine.in = new BufferedReader(new FileReader(file));
			}
		} catch (IOException e) {
			// TODO: handle exception
			e.printStackTrace();
		}
		clusteringEngine.setJobList(new ArrayList<>());
		clusteringEngine.setTaskList(new ArrayList<>());
		clusteringEngine.setTaskSubmittedList(new ArrayList<>());
		clusteringEngine.setTaskReceivedList(new ArrayList<>());
		clusteringEngine.jobSizeOfWorkflow = new HashMap<>();
		clusteringEngine.taskListOfWorkflow = new HashMap<>();
		clusteringEngine.cloudletsSubmitted = 0;
		
		// workflowEngine
		clusteringEngine.getWorkflowEngine().setJobsList(new ArrayList<>());
		clusteringEngine.getWorkflowEngine().setJobsSubmittedList(new ArrayList<>());
		clusteringEngine.getWorkflowEngine().setJobsReceivedList(new ArrayList<>());

		clusteringEngine.getWorkflowEngine().jobsSubmitted = 0;

		clusteringEngine.getWorkflowEngine().jobSizeOfWorkflow = new HashMap<>();
		clusteringEngine.getWorkflowEngine().successJobSizeOfWorkflow = new HashMap<>();
		clusteringEngine.getWorkflowEngine().startTimeOfWorkflow = new HashMap<>();
		clusteringEngine.getWorkflowEngine().finishTimeOfWorkflow = new HashMap<>();
		clusteringEngine.getWorkflowEngine().executionTimeOfWorkflow = new HashMap<>();
		
		// workflowScheduler

		clusteringEngine.getWorkflowEngine().getScheduler(0).taskQueue = new LinkedList<>();
		clusteringEngine.getWorkflowEngine().getScheduler(0).tasks = new HashMap<>();
		clusteringEngine.getWorkflowEngine().getScheduler(0).speculativeTasks = new HashMap<>();
//		clusteringEngine.getWorkflowEngine().getScheduler(0).vms = new HashMap<>();
//		clusteringEngine.getWorkflowEngine().getScheduler(0).idleTaskSlotsOfDC = new HashMap<>();
		clusteringEngine.getWorkflowEngine().getScheduler(0).taskOfJob = new HashMap<>();
		clusteringEngine.getWorkflowEngine().getScheduler(0).ackTaskOfJob = new HashMap<>();
		clusteringEngine.getWorkflowEngine().getScheduler(0).scheduledTaskOfJob = new HashMap<>();
		clusteringEngine.getWorkflowEngine().getScheduler(0).JobFactory = new HashMap<>();
		

		clusteringEngine.getWorkflowEngine().getScheduler(0).progressScores = new HashMap<>();
		clusteringEngine.getWorkflowEngine().getScheduler(0).timesTaskHasBeenRunning = new HashMap<>();
		clusteringEngine.getWorkflowEngine().getScheduler(0).progressRates = new HashMap<>();
		clusteringEngine.getWorkflowEngine().getScheduler(0).estimatedTimesToCompletion = new HashMap<>();
		
		clusteringEngine.getWorkflowEngine().getScheduler(0).lastProcessTime = 0.0;
		
		
		
		
		
		
		
		// workflow_arrival
		//generate the event send to workflowPlanner
    	for(double time:Parameters.workflowArrival.keySet()) {
//    		List<String> daxList = Parameters.workflowArrival.get(time);
    		send(getId(), time, CloudSimTags.WORKFLOW_ARRIVAL,Parameters.workflowArrival.get(time));
    	}
		
		
	}

	private void processPlanning() {
        if (Parameters.getPlanningAlgorithm().equals(PlanningAlgorithm.INVALID)) {
            return;
        }
        BasePlanningAlgorithm planner = getPlanningAlgorithm(Parameters.getPlanningAlgorithm());
        
        planner.setTaskList(getTaskList());
        planner.setVmList(getWorkflowEngine().getAllVmList());
        try {
            planner.run();
        } catch (Exception e) {
            Log.printLine("Error in configuring scheduler_method");
            e.printStackTrace();
        }
    }

    /**
     * Switch between multiple planners. Based on planner.method
     *
     * @param name the SCHMethod name
     * @return the scheduler that extends BaseScheduler
     */
    private BasePlanningAlgorithm getPlanningAlgorithm(PlanningAlgorithm name) {
        BasePlanningAlgorithm planner;

        // choose which scheduler to use. Make sure you have add related enum in
        //Parameters.java
        switch (name) {
            //by default it is FCFS_SCH
            case INVALID:
                planner = null;
                break;
            case RANDOM:
                planner = new RandomPlanningAlgorithm();
                break;
            case HEFT:
                planner = new HEFTPlanningAlgorithm();
                break;
            case DHEFT:
                planner = new DHEFTPlanningAlgorithm();
                break;
            default:
                planner = null;
                break;
        }
        return planner;
    }

    /**
     * Add impact factor for each task. This is useful in task balanced
     * clustering algorithm It is for research purpose and thus it is optional.
     *
     * @param taskList all the tasks
     */
    private void processImpactFactors(List<Task> taskList) {
        List<Task> exits = new ArrayList<>();
        for (Task task : taskList) {
            if (task.getChildList().isEmpty()) {
                exits.add(task);
            }
        }
        double avg = 1.0 / exits.size();
        for (Task task : exits) {
            addImpact(task, avg);
        }
    }

    /**
     * Add impact factor for one particular task
     *
     * @param task, the task
     * @param impact , the impact factor
     */
    private void addImpact(Task task, double impact) {

        task.setImpact(task.getImpact() + impact);
        int size = task.getParentList().size();
        if (size > 0) {
            double avg = impact / size;
            for (Task parent : task.getParentList()) {
                addImpact(parent, avg);
            }
        }
    }

    /**
     * Overrides this method when making a new and different type of Broker.
     * This method is called by {@link #body()} for incoming unknown tags.
     *
     * @param ev a SimEvent object
     * @pre ev != null
     * @post $none
     */
    protected void processOtherEvent(SimEvent ev) {
        if (ev == null) {
            Log.printLine(getName() + ".processOtherEvent(): " + "Error - an event is null.");
            return;
        }

        Log.printLine(getName() + ".processOtherEvent(): "
                + "Error - event unknown by this DatacenterBroker.");
    }

    /**
     * Send an internal event communicating the end of the simulation.
     *
     * @pre $none
     * @post $none
     */
    protected void finishExecution() {
        //sendNow(getId(), CloudSimTags.END_OF_SIMULATION);
    }

    /*
     * (non-Javadoc)
     * @see cloudsim.core.SimEntity#shutdownEntity()
     */
    @Override
    public void shutdownEntity() {
        Log.printLine(getName() + " is shutting down...");
    }

    /*
     * (non-Javadoc)
     * @see cloudsim.core.SimEntity#startEntity()
     */
    @Override
    public void startEntity() {
        Log.printLine("Starting WorkflowSim " + Parameters.getVersion());
        Log.printLine(getName() + " is starting...");
        schedule(getId(), 0, WorkflowSimTags.START_SIMULATION);
    }

    /**
     * Gets the task list.
     *
     * @return the task list
     */
    @SuppressWarnings("unchecked")
    public List<Task> getTaskList() {
        return (List<Task>) taskList;
    }

    /**
     * Sets the task list.
     *
     * @param taskList
     */
    protected void setTaskList(List<Task> taskList) {
        this.taskList = taskList;
    }
}
