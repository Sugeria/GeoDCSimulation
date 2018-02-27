package org.cloudbus.cloudsim.core.predicates;

import org.cloudbus.cloudsim.core.CloudSimTags;
import org.cloudbus.cloudsim.core.SimEvent;

public class PredicateVmProcessing extends Predicate {

	@Override
	public boolean match(SimEvent event) {
		// TODO Auto-generated method stub
		if(event.getTag() == CloudSimTags.VM_PROCESSING) {
			return true;
		}else {
			return false;
		}
	}

}
