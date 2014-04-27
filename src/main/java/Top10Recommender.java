import java.io.IOException;
import java.util.Iterator;
import java.util.TreeMap;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapred.Reducer;

public class Top10Recommender {

	public static class SOTop10Reducer extends MapReduceBase implements
			Reducer<Text, Text, Text, Text> {
		

		public void reduce(Text key, Iterator<Text> values,
				OutputCollector<Text, Text> output, Reporter reporter)
				throws IOException {
			
			TreeMap<Integer, String> repToRecordMap = new TreeMap<Integer, String>();

			// Input: UserId, List<QuestionId, score>

			while (values.hasNext()) {

				String value = values.next().toString();

				String qVal[] = value.split(",");

				repToRecordMap.put(Integer.parseInt(qVal[1]), qVal[0]);

				if (repToRecordMap.size() > 10) {
					repToRecordMap.remove(repToRecordMap.firstKey());
				}
			}

			String recQ = "";
			for (String str : repToRecordMap.descendingMap().values()) {
				recQ = recQ + str + ",";
			}
			recQ = recQ.substring(0, recQ.lastIndexOf(","));

			output.collect(key, new Text(recQ));

			// Output: UserId, Top 10 comma separated Question IDs
		}
	}
}
