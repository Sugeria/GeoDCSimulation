package org.cloudbus.cloudsim.core.predicates;

import org.cloudbus.cloudsim.core.SimEvent;
import org.workflowsim.WorkflowSimTags;

public class PredicateNoCloudletUpdate extends Predicate {

	@Override
	public boolean match(SimEvent event) {
		int tag = event.getTag();
		if(tag == WorkflowSimTags.CLOUDLET_UPDATE) {
			return false;
		}else {
			return true;
		}
	}

}
