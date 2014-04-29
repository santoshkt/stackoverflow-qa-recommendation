import java.io.IOException;
import java.util.Iterator;
import java.util.Map;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;

public class TagPreProcessing {
	public static class UserTagsReducer extends MapReduceBase implements
			Reducer<Text, Text, Text, Text> {

		public void reduce(Text key, Iterator<Text> values,
				OutputCollector<Text, Text> out, Reporter reporter)
				throws IOException {
			String str = "";
			while (values.hasNext()) {
				str = str + "" + values.next().toString() + ",";
			}
			if (str.length() > 0 && str.charAt(str.length() - 1) == ',') {
				str = str.substring(0, str.length() - 1);
			}
			out.collect(new Text("U," + key.toString()), new Text(str));
		}
	}

	public static class QuestionTagsMapper extends MapReduceBase implements
			Mapper<LongWritable, Text, Text, Text> {

		public void map(LongWritable key, Text value,
				OutputCollector<Text, Text> out, Reporter reporter)
				throws IOException {
			Map<String, String> parsed = Utils.transformXmlToMap(value
					.toString());

			String postTypeId = parsed.get("PostTypeId");
			String id = parsed.get("Id");
			String tags = parsed.get("Tags");

			if (postTypeId != null && postTypeId.equals("1")) {

				if (id != null && tags != null) {

					tags = Utils.cleanTags(tags);
					out.collect(new Text(id), new Text(tags));

				}
			}

		}

	}

	public static class QuestionTagsReducer extends MapReduceBase implements
			Reducer<Text, Text, Text, Text> {

		public void reduce(Text key, Iterator<Text> values,
				OutputCollector<Text, Text> out, Reporter reporter)
				throws IOException {
			String str = "";
			while (values.hasNext()) {
				str = str + "" + values.next().toString() + ",";
			}
			if (str.length() > 0 && str.charAt(str.length() - 1) == ',') {
				str = str.substring(0, str.length() - 1);
			}
			out.collect(new Text("Q," + key.toString()), new Text(str));

		}

	}
}
