package File_wordcount;


import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.tuple.Tuple2; // A library for getting a Tuple based on different DT.
import org.apache.flink.api.common.functions.FlatMapFunction; // Flat map function for mapping 
import org.apache.flink.util.Collector;

public class wordcountmapper implements FlatMapFunction<String, Tuple2<String, Integer>>
{
	@Override
	public void flatMap(String val,Collector<Tuple2<String,Integer>> out) 	
	{
		// Normalize and split the line
		String[] tokens = val.toLowerCase().split("\\W+");
		for (String token : tokens)
		{
			if (token.length() > 0 )
			{
				out.collect(new Tuple2<String, Integer>(token, 1)); // returns the token word count 
			}
		}
	}
	
}
