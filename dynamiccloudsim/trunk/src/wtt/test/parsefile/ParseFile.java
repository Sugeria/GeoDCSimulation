package wtt.test.parsefile;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;

import org.cloudbus.cloudsim.network.GraphReaderBrite;
import org.cloudbus.cloudsim.network.TopologicalGraph;

import de.huberlin.wbi.dcs.examples.Parameters;

public class ParseFile {

	public static void main(String[] args) {
		// TODO Auto-generated method stub
		GraphReaderBrite brite = new GraphReaderBrite();
		
		try {
			String BriteFileName = "./50.brite";
			brite.readGraphFile(BriteFileName);
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
	
	
}
