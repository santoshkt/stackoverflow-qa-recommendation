import java.io.IOException;
import java.util.Iterator;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;

public class TagSimilarity {
	public static class TagSimilarityMapper extends MapReduceBase implements
			Mapper<Text, Text, Text, Text> {

		public void map(Text key, Text value, OutputCollector<Text, Text> out,
				Reporter reporter) throws IOException {

		}
	}

	public static class TagSimilarityReducer extends MapReduceBase implements
			Reducer<Text, Text, Text, Text> {

		public void reduce(Text key, Iterator<Text> values,
				OutputCollector<Text, Text> out, Reporter reporter)
				throws IOException {
			
		}

	}
}
