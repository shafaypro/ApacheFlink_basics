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
	{ //System.out.println(val.getItemid()+ " == ")
	out.collect(val.getItem_name());  // this gets the collection item name 
	}

}
