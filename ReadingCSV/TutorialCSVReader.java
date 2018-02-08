package Reading_CSV;
import org.apache.flink.api.*;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;

import Reading_CSV.ItemMapper;
public class TutorialCSVREADER {

	public static void main(String[] args) throws Exception {
		// TODO Auto-generated method stub
		ExecutionEnvironment item_environement = ExecutionEnvironment.getExecutionEnvironment(); // importing the execution default envirenoment
		@SuppressWarnings("unused")  // Shouldn't used WArning
		// Dataset type is string because <-- the mapper Return is string
		DataSet<String> csv_reading = item_environement.readCsvFile(
				"C:\\Users\\shafay.amjad\\Desktop\\APACHE_WORKSPACE\\quickstart\\src\\main\\java\\Reading_CSV\\itemdata.csv")
		.ignoreFirstLine()  // ignore the first line in the csv file 
		.pojoType(Item.class,"item_id","item_name") // goes to an object with the var definations
		.flatMap(new ItemMapper()); // passing in the flat mapping function in the class ItemMapper() to be used for the mapping
		csv_reading.print();    // printing out the results
		//csv_reading.
		System.out.println("The csv has been read mada"); 
		
	}

}
