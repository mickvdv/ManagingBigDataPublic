package nl.utwente.importer;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;

public class ImportData {
	ArrayList<Locations> locations;
	public ImportData(){
		locations = new ArrayList<Locations>();
	}
	
	public static void main(String[] args){
		ImportData obj = new ImportData();
		obj.runLocations("./datasets/sensorLocations.txt");
		File folder = new File("./datasets/output");
		File[] listOfFiles = folder.listFiles();

		    for (int i = 0; i < listOfFiles.length; i++) {
		      if (listOfFiles[i].isFile()) {
		    	  obj.runData("./datasets/output/" + listOfFiles[i].getName());
		      }
		    }
	}
	
	 public void runData(String csvFile) {
			BufferedReader br = null;
			String line = "";
			String cvsSplitBy = ",";

			try {

				br = new BufferedReader(new FileReader(csvFile));
				while ((line = br.readLine()) != null) {

				        // use comma as separator
					line = line.replaceAll("\t", ",");
					String[] data = line.split(cvsSplitBy);
					
					for(Locations l : locations){
						if (data[0].equals(l.getId())){
							System.out.println(l.getLatitude() + "," + l.getLongitude() + "," + data[1] + "," + data[2]);
						}
					}
					

				}

			} catch (FileNotFoundException e) {
				e.printStackTrace();
			} catch (IOException e) {
				e.printStackTrace();
			} finally {
				if (br != null) {
					try {
						br.close();
					} catch (IOException e) {
						e.printStackTrace();
					}
				}
			}
		  }

	  public void runLocations(String csvFile) {

		
		BufferedReader br = null;
		String line = "";
		String cvsSplitBy = ",";

		try {

			br = new BufferedReader(new FileReader(csvFile));
			while ((line = br.readLine()) != null) {

			        // use comma as separator
				String[] data = line.split(cvsSplitBy);

				locations.add(new Locations(data[0], data[1], data[2]));

			}

		} catch (FileNotFoundException e) {
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		} finally {
			if (br != null) {
				try {
					br.close();
				} catch (IOException e) {
					e.printStackTrace();
				}
			}
		}
	  }

}
