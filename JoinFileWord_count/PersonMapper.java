package JoinFileWord_count;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.util.Collector;

import JoinFileWord_count.Person;

public class PersonMapper implements  FlatMapFunction<Person,String>{
	@Override
	// accepts two parameters one is Person object and the next is the output data type and the variable 
	public void flatMap(Person val, Collector<String> out ) throws Exception
	{
		out.collect(val.getName()); // returns the name of the specific person for the data set ! 
	}
}
