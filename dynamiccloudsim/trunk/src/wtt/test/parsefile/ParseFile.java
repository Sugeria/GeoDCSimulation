package wtt.test.parsefile;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;

import de.huberlin.wbi.dcs.examples.Parameters;

public class ParseFile {

	public static void main(String[] args) {
		// TODO Auto-generated method stub
		generateDCfailEvent();
	}
	
	private static void generateDCfailEvent() {
		// TODO Auto-generated method stub
		double dcFailTime = 1+Math.random()*(1d*24*60*60);
		int[] whetherFail = new int[Parameters.numberOfDC];
		double[] failDuration = new double[Parameters.numberOfDC];
		int failNum = 0;
		for(int dcindex = 0; dcindex < Parameters.numberOfDC; dcindex++) {
			double pro = Math.random();
			if(pro < Parameters.likelihoodOfDCFailure[dcindex]) {
				whetherFail[dcindex] = 1;
				failDuration[dcindex] = Parameters.lbOfDCFailureDuration[dcindex] + Math.random()
				*(Parameters.ubOfDCFailureDuration[dcindex] 
						- Parameters.lbOfDCFailureDuration[dcindex]);
				failNum++;
			}else {
				whetherFail[dcindex] = 0;
			}
		}
		File file = new File("./model/modelInfo-dcfail.txt");
		try {
			FileWriter out = new FileWriter(file);
			out.write(dcFailTime+"\t"+failNum+"\t");
			out.write("\r\n");
			for(int dcindex = 0; dcindex < Parameters.numberOfDC; dcindex++) {
				if(whetherFail[dcindex] == 1) {
					out.write(dcindex+"\t"+failDuration[dcindex]+"\t");
					out.write("\r\n");
				}
			}
			out.close();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
	
}
