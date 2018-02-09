package File_wordcount;

import org.apache.flink.api.*;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;

import java.util.Date;

import File_wordcount.WC; // importing the word count class
import File_wordcount.wordcountmapper;

import org.apache.flink.api.java.tuple.Tuple2; 
public class file_wordcounter {

	public static void main(String[] args) throws Exception {
		// TODO Auto-generated method stub
		//DateFormat dateFormat = new SimpleDateFormat("yyyy/MM/dd HH:mm:ss");
		Date date = new Date();
		System.out.println(date);
		ExecutionEnvironment File_Environment = ExecutionEnvironment.getExecutionEnvironment(); // importing the execution default envirenoment
		File_Environment.setParallelism(1);
		DataSet<Tuple2<String, Integer>> counts  = File_Environment
		.readTextFile("C:\\Users\\shafay.amjad\\Desktop\\APACHE_WORKSPACE\\quickstart\\src\\main\\java\\File_wordcount\\book.txt").
		flatMap(new wordcountmapper())
		.groupBy(0)
		.sum(1); // reading the text file using the constant envirenoment.
		
		// Working below
		counts.writeAsCsv("C:\\Users\\shafay.amjad\\Desktop\\APACHE_WORKSPACE\\quickstart\\src\\main\\java\\File_wordcount\\outputfrom_wordcount.csv", "\n", " ");
		System.out.println(date);
		counts.print();
	}
	
	
}
