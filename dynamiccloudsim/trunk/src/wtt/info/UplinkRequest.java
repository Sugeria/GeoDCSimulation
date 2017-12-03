package wtt.info;


import de.huberlin.wbi.dcs.workflow.Task;

public class UplinkRequest {
	public Task task;
	public int dataindex;
	public double requestedUpbandwidth;
	public boolean isSuccess;
	
	public UplinkRequest(Task task, double up, int dataindex) {
		this.dataindex = dataindex;
		this.task = task;
		this.requestedUpbandwidth = up;
	}
}
