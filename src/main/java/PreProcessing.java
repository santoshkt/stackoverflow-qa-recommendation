import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.Map;

public class PreProcessing {
	public static class UserQuestionPairMapper extends MapReduceBase implements
			Mapper<LongWritable, Text, Text, Text> {

		public void map(LongWritable key, Text value,
				OutputCollector<Text, Text> output, Reporter reporter)
				throws IOException {

			// Input: XML String of the format <row key="value" />

			// Parse the input string into a nice map
			Map<String, String> parsed = Utils.transformXmlToMap(value
					.toString());

			// Grab the "Text" field, since that is what we are counting over
			String postTypeId = parsed.get("PostTypeId");
			String parentId = parsed.get("ParentId");
			String ownerUserId = parsed.get("OwnerUserId");

			if (postTypeId != null && postTypeId.equals("2")) {

				if (parentId != null && ownerUserId != null) {
					output.collect(new Text(ownerUserId), new Text(parentId));
				}
			}

			// Output: A,UserId,QuestionId
		}

	}

	public static class QuestionQuestionPairReducer extends MapReduceBase
			implements Reducer<Text, Text, Text, IntWritable> {

		public void reduce(Text key, Iterator<Text> values,
				OutputCollector<Text, IntWritable> output, Reporter reporter)
				throws IOException {

			ArrayList<String> questions = new ArrayList<String>();
			while (values.hasNext()) {
				questions.add(values.next().toString());
			}

			Collections.sort(questions);

			// For each user, write pairs of the questions he rated.
			// eg: question1,question2 count
			for (String question1 : questions) {
				for (String question2 : questions) {
					if (question1.compareTo(question2) < 0)
						output.collect(new Text("A,"+question1 + "," + question2),
								new IntWritable(1));
				}
			}
		}
	}

	public static class QuestionQuestionSumMapper extends MapReduceBase
			implements Mapper<Text, Text, Text, IntWritable> {

		public void map(Text key, Text value,
				OutputCollector<Text, IntWritable> output, Reporter reporter)
				throws IOException {

			// Input: qid1,qid2 1
			output.collect(key, new IntWritable(Integer.parseInt(value.toString())));
			// Output: qid1,qid2 1
		}

	}

	public static class QuestionQuestionSumReducer extends MapReduceBase
			implements Reducer<Text, IntWritable, Text, IntWritable> {
		public void reduce(Text key, Iterator<IntWritable> values,
				OutputCollector<Text, IntWritable> output, Reporter reporter)
				throws IOException {

			int sum = 0;
			while (values.hasNext()) {
				sum = sum + values.next().get();
			}

			output.collect(key, new IntWritable(sum));

		}
	}
	
	public static class UserIDQuestionPairMatrix extends MapReduceBase
			implements Mapper<LongWritable,Text,Text,IntWritable> {
		
				public void map(LongWritable key, Text value,
				OutputCollector<Text, IntWritable> output, Reporter reporter)
				throws IOException {

			// Input: XML String of the format <row key="value" />

			// Parse the input string into a nice map
			Map<String, String> parsed = Utils.transformXmlToMap(value
					.toString());

			// Grab the "Text" field, since that is what we are counting over
			String postTypeId = parsed.get("PostTypeId");
			String parentId = parsed.get("ParentId");
			String ownerUserId = parsed.get("OwnerUserId");

			if (postTypeId != null && postTypeId.equals("2")) {

				if (parentId != null && ownerUserId != null) {
					output.collect(new Text("B,"+ownerUserId+","+parentId), new IntWritable(1));
				}
			}

			// Output: B,UserId, QuestionId 1
		}
	}

}
