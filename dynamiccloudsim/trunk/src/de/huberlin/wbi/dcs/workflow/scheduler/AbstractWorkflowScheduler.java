package de.huberlin.wbi.dcs.workflow.scheduler;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Queue;

import org.cloudbus.cloudsim.Cloudlet;
import org.cloudbus.cloudsim.DatacenterBroker;
import org.cloudbus.cloudsim.Host;
import org.cloudbus.cloudsim.Log;
import org.cloudbus.cloudsim.Vm;
import org.cloudbus.cloudsim.core.CloudSim;
import org.cloudbus.cloudsim.core.CloudSimTags;
import org.cloudbus.cloudsim.core.SimEvent;

import com.mathworks.toolbox.javabuilder.MWClassID;
import com.mathworks.toolbox.javabuilder.MWComplexity;
import com.mathworks.toolbox.javabuilder.MWNumericArray;
import com.sun.javafx.sg.prism.web.NGWebView;

import EDU.oswego.cs.dl.util.concurrent.FJTask.Par;
import de.huberlin.wbi.cuneiform.core.semanticmodel.Param;
import de.huberlin.wbi.dcs.DynamicHost;
import de.huberlin.wbi.dcs.DynamicVm;
import de.huberlin.wbi.dcs.examples.Parameters;
import de.huberlin.wbi.dcs.workflow.DataDependency;
import de.huberlin.wbi.dcs.workflow.Task;
import de.huberlin.wbi.dcs.workflow.Workflow;
import sun.security.provider.JavaKeyStore.CaseExactJKS;
import taksassign.TaskAssign;

public abstract class AbstractWorkflowScheduler extends DatacenterBroker
		implements WorkflowScheduler {

	private List<Workflow> workflows;
	private Map<Integer, Vm> vms;
	private int taskSlotsPerVm;
	//private Queue<Vm> idleTaskSlots;
	private Map<Integer, LinkedList<Vm>> idleTaskSlotsOfDC;
	private double runtime;

	// two collections of tasks, which are currently running;
	// note that the second collections is a subset of the first collection
	private Map<Integer, Task> tasks;
	private Map<Integer, Task> speculativeTasks;
	
	

	public AbstractWorkflowScheduler(String name, int taskSlotsPerVm)
			throws Exception {
		super(name);
		workflows = new ArrayList<>();
		vms = new HashMap<>();
		this.taskSlotsPerVm = taskSlotsPerVm;
		//idleTaskSlots = new LinkedList<>();
		idleTaskSlotsOfDC = new HashMap<>();
		tasks = new HashMap<>();
		speculativeTasks = new HashMap<>();
		
		
	}
	
	
	
	
	
	
	
	

	public List<Workflow> getWorkflows() {
		return workflows;
	}

	public int getTaskSlotsPerVm() {
		return taskSlotsPerVm;
	}

	public void submitWorkflow(Workflow workflow) {
		workflows.add(workflow);
	}
	
	public Map<Integer, LinkedList<Vm>> getIdleTaskSlotsOfDC(){
		return idleTaskSlotsOfDC;
	}

	private void submitTask(Task task, Vm vm) {
		Log.printLine(CloudSim.clock() + ": " + getName() + ": VM # "
				+ vm.getId() + " starts executing Task # "
				+ task.getCloudletId() + " \"" + task.getName() + " "
				+ task.getParams() + " \"");
		task.setVmId(vm.getId());
		if (Parameters.numGen.nextDouble() < getLikelihoodOfFailureOfDC().get(task.getAssignmentDCId())) {
			task.setScheduledToFail(true);
			task.setCloudletLength((long) (task.getCloudletLength() * getRuntimeFactorIncaseOfFailureOfDC().get(task.getAssignmentDCId())));
		} else {
			task.setScheduledToFail(false);
		}
		sendNow(getVmsToDatacentersMap().get(vm.getId()),
				CloudSimTags.CLOUDLET_SUBMIT, task);
	}

	private void submitSpeculativeTask(Task task, Vm vm) {
		Log.printLine(CloudSim.clock() + ": " + getName() + ": VM # "
				+ vm.getId() + " starts executing speculative copy of Task # "
				+ task.getCloudletId() + " \"" + task.getName() + " "
				+ task.getParams() + " \"");
		task.setVmId(vm.getId());
		if (Parameters.numGen.nextDouble() < getLikelihoodOfFailureOfDC().get(task.getAssignmentDCId())) {
			task.setScheduledToFail(true);
			task.setCloudletLength((long) (task.getCloudletLength() * getRuntimeFactorIncaseOfFailureOfDC().get(task.getAssignmentDCId())));
		} else {
			task.setScheduledToFail(false);
		}
		sendNow(getVmsToDatacentersMap().get(vm.getId()),
				CloudSimTags.CLOUDLET_SUBMIT, task);
	}

	/**
	 * Submit cloudlets to the created VMs. This function is called after Vms
	 * have been created.
	 * 
	 * @pre $none
	 * @post $none
	 */
	@Override
	protected void submitCloudlets() {
		for (Vm vm : getVmsCreatedList()) {
			vms.put(vm.getId(), vm);
			for (int i = 0; i < getTaskSlotsPerVm(); i++) {
				Integer dcId = getVmsToDatacentersMap().get(vm.getId());
				if (!getIdleTaskSlotsOfDC().containsKey(dcId)) {
					getIdleTaskSlotsOfDC().put(dcId, new LinkedList<>());
					getIdleTaskSlotsOfDC().get(dcId).add(vm);
				}else {
					getIdleTaskSlotsOfDC().get(dcId).add(vm);
				}
				//idleTaskSlots.add(vm);
			}
		}
		for (Workflow workflow : workflows) {
			Collection<Task> tasks = workflow.getTasks();
			reschedule(tasks, vms.values());
			for (Task task : tasks) {
				if (task.readyToExecute()) {
					taskReady(task);
				}
			}
		}
		submitTasks();
	}

	protected void submitTasks() {
		Queue<Vm> taskSlotsKeptIdle = new LinkedList<>();
		// compute the task assignment among datacenters for ready tasks
		TaskAssign taskassign = null;
		taskassign = new TaskAssign();
		Integer numberOfTask = getTaskQueue().size();
		
		MWNumericArray tasknum = null;
		MWNumericArray dcnum = null;
		MWNumericArray probArray = null;
		MWNumericArray allDuraArray = null;
		MWNumericArray data = null;
		MWNumericArray datapos = null;
		MWNumericArray bandwidth = null;
		MWNumericArray SlotArray = null;
		MWNumericArray UpArray = null;
		MWNumericArray DownArray = null;
		MWNumericArray iteration_bound = null;
		Object[] result = null;	/* Stores the result */
		MWNumericArray x = null;	/* Location of minimal value */
		MWNumericArray flag = null;	/* solvable flag */
		
		int[] dims = {1,1};
		tasknum = MWNumericArray.newInstance(dims, MWClassID.INT64,MWComplexity.REAL);
		dcnum = MWNumericArray.newInstance(dims, MWClassID.INT64, MWComplexity.REAL);
		iteration_bound = MWNumericArray.newInstance(dims, MWClassID.INT64, MWComplexity.REAL);
		dims[0] = Parameters.numberOfDC;
		dims[1] = 4;
		probArray = MWNumericArray.newInstance(dims, MWClassID.DOUBLE, MWComplexity.REAL);
		dims[0] = numberOfTask * Parameters.numberOfDC;
		allDuraArray = MWNumericArray.newInstance(dims, MWClassID.DOUBLE, MWComplexity.REAL);
		dims[0] = 1;
		dims[1] = numberOfTask;
		data = MWNumericArray.newInstance(dims, MWClassID.INT64, MWComplexity.REAL);
		dims[0] = numberOfTask;
		dims[1] = Parameters.ubOfData;
		datapos = MWNumericArray.newInstance(dims, MWClassID.INT64, MWComplexity.REAL);
		dims[0] = numberOfTask * Parameters.numberOfDC;
		bandwidth = MWNumericArray.newInstance(dims, MWClassID.DOUBLE, MWComplexity.REAL);
		dims[0] = 1;
		dims[1] = Parameters.numberOfDC;
		SlotArray = MWNumericArray.newInstance(dims, MWClassID.INT64, MWComplexity.REAL);
		UpArray = MWNumericArray.newInstance(dims, MWClassID.DOUBLE, MWComplexity.REAL);
		DownArray = MWNumericArray.newInstance(dims, MWClassID.DOUBLE, MWComplexity.REAL);
		
		
		// set value to the above arguments
		
		tasknum.set(1, numberOfTask);
		dcnum.set(1, Parameters.numberOfDC);
		iteration_bound.set(1, Parameters.boundOfIter);
		
		//probArray
		for (int dcindex = 0; dcindex < Parameters.numberOfDC; dcindex++) {
			for (int iterm = 0; iterm < 4; iterm++) {
				int[] pos = {dcindex,iterm};
				double value = 0d;
				switch (iterm) {
				case 0:
					value = (1-Parameters.likelihoodOfDCFailure[dcindex])*(1-Parameters.likelihoodOfFailure[dcindex])*(1-Parameters.likelihoodOfStragglerOfDC[dcindex]);
					break;
				case 1:
					value = (1-Parameters.likelihoodOfDCFailure[dcindex])*(1-Parameters.likelihoodOfFailure[dcindex])*Parameters.likelihoodOfStragglerOfDC[dcindex];
					break;
				case 2:
					value = (1-Parameters.likelihoodOfDCFailure[dcindex])*Parameters.likelihoodOfFailure[dcindex];
					break;
				case 3:
					value = Parameters.likelihoodOfDCFailure[dcindex];
					break;
				default:
					break;
				}
				probArray.set(pos,value);
			}
		}
		
		//data datapos 
		double[] Totaldatasize = new double[numberOfTask];
		double[][] datasize = new double[numberOfTask][Parameters.ubOfData];
		LinkedList<Task> ReadyTasks = (LinkedList<Task>)getTaskQueue();
		int[] pos = new int[2];
		for (int tindex = 0; tindex < numberOfTask; tindex++) {
			Task task = ReadyTasks.get(tindex);
			pos[0] = 1;
			pos[1] = tindex;
			data.set(pos, task.numberOfData);
			Totaldatasize[tindex] = 0d;
			for(int dataindex = 0; dataindex < task.numberOfData; dataindex++) {
				pos[0] = tindex;
				pos[1] = dataindex;
				datapos.set(pos, task.positionOfData[dataindex]);
				datasize[tindex][dataindex] = task.sizeOfData[dataindex];
				Totaldatasize[tindex] += task.sizeOfData[dataindex];
			}
		}
		
		//bandwidth allDuraArray
		for (int tindex = 0; tindex < numberOfTask; tindex++) {
			Task task = ReadyTasks.get(tindex);
			for (int dcindex = 0; dcindex < Parameters.numberOfDC; dcindex++) {
				int xindex = (tindex - 1)*Parameters.numberOfDC + dcindex;
				pos[0] = 1;
				pos[1] = tindex;
				int datanumber = data.getInt(pos);
				double[] datasizeOfTask = datasize[tindex];
				double TotaldatasizeOfTask = Totaldatasize[tindex];
				for(int dataindex = 0; dataindex < datanumber; dataindex++) {
					pos[0] = tindex;
					pos[1] = dataindex;
					if (datapos.getInt(pos) == dcindex) {
						TotaldatasizeOfTask -= datasizeOfTask[dataindex];
						datasizeOfTask[dataindex] = 0;
					}
				}
				for(int dataindex = 0; dataindex < datanumber; dataindex++) {
					pos[0] = xindex;
					pos[1] = dataindex;
					bandwidth.set(pos, Parameters.bwBaselineOfDC[dcindex]*datasizeOfTask[dataindex]/TotaldatasizeOfTask);
				}
				for (int iterm = 0; iterm < 4; iterm++) {
					pos[0] = xindex;
					pos[1] = iterm;
					double value = 0d;
					switch (iterm) {
					case 0:
						value = task.getMi()/Parameters.MIPSbaselineOfDC[dcindex]
								+ TotaldatasizeOfTask/Parameters.bwBaselineOfDC[dcindex]
								+ 2*task.getIo()/Parameters.ioBaselineOfDC[dcindex];
						break;
					case 1:
						value = task.getMi()/(Parameters.MIPSbaselineOfDC[dcindex]*Parameters.stragglerPerformanceCoefficientOfDC[dcindex])
								+ TotaldatasizeOfTask/(Parameters.bwBaselineOfDC[dcindex]*Parameters.stragglerPerformanceCoefficientOfDC[dcindex])
								+ 2*task.getIo()/(Parameters.ioBaselineOfDC[dcindex]*Parameters.stragglerPerformanceCoefficientOfDC[dcindex]);
						break;
					case 2:
						value = (Parameters.runtimeFactorInCaseOfFailure[dcindex] + 1)
								* task.getMi()/Parameters.MIPSbaselineOfDC[dcindex]
								+ TotaldatasizeOfTask/Parameters.bwBaselineOfDC[dcindex]
								+ 2*task.getIo()/Parameters.ioBaselineOfDC[dcindex];
						break;
					case 3:
						value = 2
								* task.getMi()/Parameters.MIPSbaselineOfDC[dcindex]
								+ TotaldatasizeOfTask/Parameters.bwBaselineOfDC[dcindex]
								+ 2*task.getIo()/Parameters.ioBaselineOfDC[dcindex];
						break;
					default:
						break;
					}
					allDuraArray.set(pos,value);
				}
			}
			
		}
		
		//SlotArray UpArray DownArray
		
		for(int dcindex = 0; dcindex < Parameters.numberOfDC; dcindex++) {
			pos[0] = 1;
			pos[1] = dcindex;
			SlotArray.set(pos, idleTaskSlotsOfDC.get(dcindex + DCbase).size());
			UpArray.set(pos, getUplinkOfDC().get(dcindex + DCbase));
			DownArray.set(pos, getDownlinkOfDC().get(dcindex + DCbase));
		}
		
		
		
		
		
		result = taskassign.command(11, tasknum,dcnum,probArray,allDuraArray,data,datapos,bandwidth,SlotArray,UpArray,DownArray,iteration_bound);
		x = (MWNumericArray)result[0];
		double[] xd = x.getDoubleData();
		flag = (MWNumericArray)result[1];
		int flagi = flag.getInt();
		
		
		
		while (tasksRemaining() && !idleTaskSlots.isEmpty()) {
			Vm vm = idleTaskSlots.remove();
			Task task = getNextTask(vm);
			// task will be null if scheduler has tasks to be executed, but not
			// for this VM (e.g., if it abides to a static schedule or this VM
			// is a straggler, which the scheduler does not want to assign tasks
			// to)
			if (task == null) {
				taskSlotsKeptIdle.add(vm);
			} else if (tasks.containsKey(task.getCloudletId())) {
				speculativeTasks.put(task.getCloudletId(),
						task);
				submitSpeculativeTask(task, vm);
			} else {
				tasks.put(task.getCloudletId(), task);
				submitTask(task, vm);
			}
		}
		idleTaskSlots.addAll(taskSlotsKeptIdle);
	}

	protected void clearDatacenters() {
		for (Vm vm : getVmsCreatedList()) {
			if (vm instanceof DynamicVm) {
				DynamicVm dVm = (DynamicVm) vm;
				dVm.closePerformanceLog();
			}
		}
		super.clearDatacenters();
		runtime = CloudSim.clock();
	}

	public double getRuntime() {
		return runtime;
	}

	protected void resetTask(Task task) {
		task.setCloudletFinishedSoFar(0);
		try {
			task.setCloudletStatus(Cloudlet.CREATED);
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	@Override
	protected void processCloudletReturn(SimEvent ev) {
		// determine what kind of task was finished,
		Task task = (Task) ev.getData();
		Vm vm = vms.get(task.getVmId());
		Host host = vm.getHost();

		// if the task finished successfully, cancel its copy and remove them
		// both from internal data structures
		if (task.getCloudletStatus() == Cloudlet.SUCCESS) {
			Task originalTask = tasks.remove(task.getCloudletId());
			Task speculativeTask = speculativeTasks
					.remove(task.getCloudletId());

			if ((task.isSpeculativeCopy() && speculativeTask == null)
					|| originalTask == null) {
				return;
			}

			if (task.isSpeculativeCopy()) {
				Log.printLine(CloudSim.clock() + ": " + getName() + ": VM # "
						+ speculativeTask.getVmId()
						+ " completed speculative copy of Task # "
						+ speculativeTask.getCloudletId() + " \""
						+ speculativeTask.getName() + " "
						+ speculativeTask.getParams() + " \"");
				Log.printLine(CloudSim.clock() + ": " + getName() + ": VM # "
						+ originalTask.getVmId() + " cancelled Task # "
						+ originalTask.getCloudletId() + " \""
						+ originalTask.getName() + " "
						+ originalTask.getParams() + " \"");
				vms.get(originalTask.getVmId()).getCloudletScheduler()
						.cloudletCancel(originalTask.getCloudletId());
			} else {
				Log.printLine(CloudSim.clock() + ": " + getName() + ": VM # "
						+ originalTask.getVmId() + " completed Task # "
						+ originalTask.getCloudletId() + " \""
						+ originalTask.getName() + " "
						+ originalTask.getParams() + " \"");
				if (speculativeTask != null) {
					Log.printLine(CloudSim.clock() + ": " + getName()
							+ ": VM # " + speculativeTask.getVmId()
							+ " cancelled speculative copy of Task # "
							+ speculativeTask.getCloudletId() + " \""
							+ speculativeTask.getName() + " "
							+ speculativeTask.getParams() + " \"");
					vms.get(speculativeTask.getVmId()).getCloudletScheduler()
							.cloudletCancel(speculativeTask.getCloudletId());
				}
			}

			// free task slots occupied by finished / cancelled tasks
			Vm originalVm = vms.get(originalTask.getVmId());
			idleTaskSlots.add(originalVm);
			taskSucceeded(originalTask, originalVm);
			if (speculativeTask != null) {
				Vm speculativeVm = vms.get(speculativeTask.getVmId());
				idleTaskSlots.add(speculativeVm);
				taskFailed(speculativeTask, speculativeVm);
			}

			// update the task queue by traversing the successor nodes in the
			// workflow
			for (DataDependency outgoingEdge : originalTask.getWorkflow()
					.getGraph().getOutEdges(originalTask)) {
				if (host instanceof DynamicHost) {
					DynamicHost dHost = (DynamicHost) host;
					dHost.addFile(outgoingEdge.getFile());
				}
				Task child = originalTask.getWorkflow().getGraph()
						.getDest(outgoingEdge);
				child.decNDataDependencies();
				if (child.readyToExecute()) {
					taskReady(child);
				}
			}

			// if the task finished unsuccessfully,
			// if it was a speculative task, just leave it be
			// otherwise, -- if it exists -- the speculative task becomes the
			// new original
		} else {
			Task speculativeTask = speculativeTasks
					.remove(task.getCloudletId());
			if (task.isSpeculativeCopy()) {
				Log.printLine(CloudSim.clock()
						+ ": "
						+ getName()
						+ ": VM # "
						+ task.getVmId()
						+ " encountered an error with speculative copy of Task # "
						+ task.getCloudletId() + " \"" + task.getName() + " "
						+ task.getParams() + " \"");
			} else {
				Log.printLine(CloudSim.clock() + ": " + getName() + ": VM # "
						+ task.getVmId() + " encountered an error with Task # "
						+ task.getCloudletId() + " \"" + task.getName() + " "
						+ task.getParams() + " \"");
				tasks.remove(task.getCloudletId());
				if (speculativeTask != null) {
					speculativeTask.setSpeculativeCopy(false);
					tasks.put(speculativeTask.getCloudletId(), speculativeTask);
				} else {
					resetTask(task);
					taskReady(task);
				}
			}
			idleTaskSlots.add(vm);
			taskFailed(task, vm);
		}

		if (tasksRemaining()) {
			submitTasks();
		} else if (idleTaskSlots.size() == getVmsCreatedList().size()
				* getTaskSlotsPerVm()) {
			Log.printLine(CloudSim.clock() + ": " + getName()
					+ ": All Tasks executed. Finishing...");
			terminate();
			clearDatacenters();
			finishExecution();
		}
	}

}
