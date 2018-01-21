package org.cloudbus.cloudsim.core.predicates;

import org.cloudbus.cloudsim.core.CloudSimTags;
import org.cloudbus.cloudsim.core.SimEvent;

public class PredicateNoCloudletSubmitAck extends Predicate {

	@Override
	public boolean match(SimEvent event) {
		int tag = event.getTag();
		if(tag == CloudSimTags.CLOUDLET_SUBMIT_ACK) {
			return false;
		}else {
			return true;
		}
	}

}
