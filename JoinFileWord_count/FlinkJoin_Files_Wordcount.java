package JoinFileWord_count;
import org.apache.flink.api.java.DataSet; // loading inthe data set libaray !  !
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.tuple.Tuple2;

import JoinFileWord_count.Person; // person class having the set and the get for the column attributes . 
import JoinFileWord_count.PersonMapper; // mapper for hte person1

public class FlinkJoin_Files_Wordcount {
	 public static void main(String[] args) throws Exception
	 	{
				ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment(); // creating in the execution envirenoment .
				// read c.s.v. one
//				DataSet<Person> csv_Data_1=  env.readCsvFile("C:\\Users\\shafay.amjad\\Desktop\\APACHE_WORKSPACE\\quickstart\\src\\main\\java\\JoinFileWord_count\\itemdata.csv").pojoType(Person.class,"name","age","zipcode");
//				// read c.s.v. two. 
//				DataSet<Person> csv_Data_2 = env.readCsvFile("C:\\Users\\shafay.amjad\\Desktop\\APACHE_WORKSPACE\\quickstart\\src\\main\\java\\JoinFileWord_count\\itemdata2.csv").pojoType(Person.class, "name","age","zipcode");
//				
//				// join both of the csv's
//				// Has two person based Inputs and the outputs 
//				//joins the one c.s.v with the 2nd c.s.v  on the basis of the attributes being matched. 
//				DataSet<Tuple2<Person,Person>> result = csv_Data_1.join(csv_Data_2)
//						.where("name")
//						.equalTo("name");
//				
//				DataSet<String> finalResult = result
//						.flatMap(new PersonMapper());
				
				// The Short way for having the union result 
				
				
				
				DataSet<Person> unionObject1 = env.readCsvFile("C:\\Users\\shafay.amjad\\Desktop\\APACHE_WORKSPACE\\quickstart\\src\\main\\java\\JoinFileWord_count\\itemdata.csv")
						.ignoreFirstLine()
						.pojoType(Person.class, "name", "age", "zipcode");
				System.out.println("I am living like a rockstar  ");
				DataSet<Person> unionObject2 = env.readCsvFile("C:\\Users\\shafay.amjad\\Desktop\\APACHE_WORKSPACE\\quickstart\\src\\main\\java\\JoinFileWord_count\\itemdata2.csv")
		                .ignoreFirstLine()
						.pojoType(Person.class, "name", "age", "zipcode");
				
				DataSet<Person> unionResult =unionObject1.union(unionObject2);
				
				DataSet<String> unionResultfile =unionResult
					       .flatMap(new PersonMapper());
				
				unionResultfile.print();
	 
	 	}
}
