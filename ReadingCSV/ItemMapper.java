/**
 * 
 */
package Reading_CSV;
import org.apache.flink.api.common.functions.FlatMapFunction; // for the usage of the flat map function
import org.apache.flink.util.Collector;  // for the usage of the collector. 1


/**
 * @author Shafay.Amjad
 *
 */

public class ItemMapper implements FlatMapFunction<Item,String>{
@Override
public void flatMap(Item val , Collector<String> out) throws Exception
	// The function has the Item <-- as an input object(datatype) and a collection output which is "String based"
	{ //System.out.println(val.getItemid()+ " == ")
	out.collect(val.getItem_name());  // this gets the collection item name 
	// one by one the collection is passed (a value from the varaiable of the Item Object (function get ITemname)
	}

}
