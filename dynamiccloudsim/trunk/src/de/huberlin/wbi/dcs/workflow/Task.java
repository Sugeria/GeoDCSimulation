package de.huberlin.wbi.dcs.workflow;

import java.util.ArrayList;
import java.util.List;

import org.cloudbus.cloudsim.Consts;
import org.cloudbus.cloudsim.UtilizationModel;
import org.workflowsim.FileItem;

import de.huberlin.wbi.dcs.HeterogeneousCloudlet;
import de.huberlin.wbi.dcs.examples.Parameters;

public class Task extends HeterogeneousCloudlet implements Comparable<Task> {
	
	private final String name;
	
	private final String params;
	
	private int nDataDependencies;
	
	private Workflow workflow;
	
	
	private static int maxDepth;
	
	private boolean speculativeCopy;
	
	private boolean destinedToFail;
	
    /*
     * The list of parent tasks. 
     */
    private List<Task> parentList;
    /*
     * The list of child tasks. 
     */
    private List<Task> childList;
    /*
     * The list of all files (input data and ouput data)
     */
    private List<FileItem> fileList;
    /*
     * The priority used for research. Not used in current version. 
     */
    private int priority;
    /*
     * The depth of this task. Depth of a task is defined as the furthest path 
     * from the root task to this task. It is set during the workflow parsing 
     * stage. 
     */
    private int depth;
    /*
     * The impact of a task. It is used in research. 
     */
    private double impact;

    /*
     * The type of a task. 
     */
    private String type;
    

    /**
     * The finish time of a task (Because cloudlet does not allow WorkflowSim to
     * update finish_time)
     */
    private double taskFinishTime;
    
    public int jobId;
	

    
    public int workflowId;
	
	public Task(
			final String name,
			final String params,
			final int userId,
			final int cloudletId,
			final long miLength,
			final long ioLength,
			final long bwLength,
			final int pesNumber,
			final long cloudletFileSize,
			final long cloudletOutputSize,
			final UtilizationModel utilizationModelCpu,
			final UtilizationModel utilizationModelRam,
			final UtilizationModel utilizationModelBw,
			final int submitDCIndex) {
		super(
				cloudletId,
				miLength,
				ioLength,
				bwLength,
				pesNumber,
				cloudletFileSize,
				cloudletOutputSize,
				utilizationModelCpu,
				utilizationModelRam,
				utilizationModelBw);
		this.name = name;
		this.params=params;
//		this.workflow = workflow;
		this.setUserId(userId);
		this.depth = 0;
		destinedToFail = false;
		speculativeCopy = false;
		
		this.childList = new ArrayList<>();
        this.parentList = new ArrayList<>();
        this.fileList = new ArrayList<>();
        this.impact = 0.0;
        this.taskFinishTime = -1.0;
        this.submitDCIndex = submitDCIndex;
		
	}
	
	public Task(Task task) {
		this(task.getName(),
				task.getParams(),
//				task.getWorkflow(),
				task.getUserId(),
				task.getCloudletId(),
				task.getMi(),
				task.getIo(),
				task.getBw(),
				task.getNumberOfPes(),
				task.getCloudletFileSize(),
				task.getCloudletOutputSize(),
				task.getUtilizationModelCpu(),
				task.getUtilizationModelRam(),
				task.getUtilizationModelBw(),
				task.submitDCIndex);
		
	}
	
	/**
     * Sets the type of the task
     *
     * @param type the type
     */
    public void setType(String type) {
        this.type = type;
    }

    /**
     * Gets the type of the task
     *
     * @return the type of the task
     */
    public String getType() {
        return type;
    }

    /**
     * Sets the priority of the task
     *
     * @param priority the priority
     */
    public void setPriority(int priority) {
        this.priority = priority;
    }


    /**
     * Gets the priority of the task
     *
     * @return the priority of the task
     * @pre $none
     * @post $none
     */
    public int getPriority() {
        return this.priority;
    }

    
    /**
     * Gets the child list of the task
     *
     * @return the list of the children
     */
    public List<Task> getChildList() {
        return this.childList;
    }

    /**
     * Sets the child list of the task
     *
     * @param list, child list of the task
     */
    public void setChildList(List<Task> list) {
        this.childList = list;
    }

    /**
     * Sets the parent list of the task
     *
     * @param list, parent list of the task
     */
    public void setParentList(List<Task> list) {
        this.parentList = list;
    }

    /**
     * Adds the list to existing child list
     *
     * @param list, the child list to be added
     */
    public void addChildList(List<Task> list) {
        this.childList.addAll(list);
    }

    /**
     * Adds the list to existing parent list
     *
     * @param list, the parent list to be added
     */
    public void addParentList(List<Task> list) {
        this.parentList.addAll(list);
    }

    /**
     * Gets the list of the parent tasks
     *
     * @return the list of the parents
     */
    public List<Task> getParentList() {
        return this.parentList;
    }

    /**
     * Adds a task to existing child list
     *
     * @param task, the child task to be added
     */
    public void addChild(Task task) {
        this.childList.add(task);
    }

    /**
     * Adds a task to existing parent list
     *
     * @param task, the parent task to be added
     */
    public void addParent(Task task) {
        this.parentList.add(task);
    }
    
    /**
     * Gets the list of the files
     *
     * @return the list of files
     * @pre $none
     * @post $none
     */
    public List<FileItem> getFileList() {
        return this.fileList;
    }

    /**
     * Adds a file to existing file list
     *
     * @param file, the file to be added
     */
    public void addFile(FileItem file) {
        this.fileList.add(file);
    }

    /**
     * Sets a file list
     *
     * @param list, the file list
     */
    public void setFileList(List<FileItem> list) {
        this.fileList = list;
    }
    
    
    
    
    
	
    /**
     * Sets the impact factor
     *
     * @param impact, the impact factor
     */
    public void setImpact(double impact) {
        this.impact = impact;
    }

    /**
     * Gets the impact of the task
     *
     * @return the impact of the task
     * @pre $none
     * @post $none
     */
    public double getImpact() {
        return this.impact;
    }

    /**
     * Sets the finish time of the task (different to the one used in Cloudlet)
     *
     * @param time finish time
     */
    public void setTaskFinishTime(double time) {
        this.taskFinishTime = time;
    }

    /**
     * Gets the finish time of a task (different to the one used in Cloudlet)
     *
     * @return
     */
    public double getTaskFinishTime() {
        return this.taskFinishTime;
    }

    /**
     * Gets the total cost of processing or executing this task The original
     * getProcessingCost does not take cpu cost into it also the data file in
     * Task is stored in fileList <tt>Processing Cost = input data transfer +
     * processing cost + output transfer cost</tt> .
     *
     * @return the total cost of processing Cloudlet
     * @pre $none
     * @post $result >= 0.0
     */
    @Override
    public double getProcessingCost() {
        // cloudlet cost: execution cost...

        double cost = getCostPerSec() * getActualCPUTime();

        // ...plus input data transfer cost...
        long fileSize = 0;
        for (FileItem file : getFileList()) {
            fileSize += file.getSize() / Consts.MILLION;
        }
        cost += costPerBw * fileSize;
        return cost;
    }
	
	
	
	
	
	public String getName() {
		return name;
	}
	
	public String getParams() {
		return params;
	}
	
	public String toString() {
		return getName();
	}
	
	public void incNDataDependencies() {
		nDataDependencies++;
	}
	
	public void decNDataDependencies() {
		nDataDependencies--;
	}
	
	public boolean readyToExecute() {
		return nDataDependencies == 0;
	}
	
	public Workflow getWorkflow() {
		return workflow;
	}
	
	public static int getMaxDepth() {
		return maxDepth;
	}
	
	public int getDepth() {
		return depth;
	}
	
	public void setDepth(int depth) {
		this.depth = depth;
		if (depth > maxDepth) {
			maxDepth = depth;
		}
	}
	
	@Override
	public int compareTo(Task o) {
		return (this.getDepth() == o.getDepth()) ? Double.compare(this.getCloudletId(), o.getCloudletId()) : Double.compare(this.getDepth(), o.getDepth());
	}
	
	@Override
    public boolean equals(Object arg0) {
		return ((Task)arg0).getCloudletId() == getCloudletId();
	}
	
	@Override
    public int hashCode() {
		return getCloudletId();
    }

	public void setScheduledToFail(boolean scheduledToFail) {
		this.destinedToFail = scheduledToFail;
	}
	
	public boolean isScheduledToFail() {
		return destinedToFail;
	}
	
	public void setSpeculativeCopy(boolean speculativeCopy) {
		this.speculativeCopy = speculativeCopy;
	}
	
	public boolean isSpeculativeCopy() {
		return speculativeCopy;
	}
	
	
	
}
