import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
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
			if(ownerUserId == null){
				ownerUserId = parsed.get("OwnerDisplayName");
				if(ownerUserId != null){
					ownerUserId = ownerUserId.replace(" ", "_");
				}
			}

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
			output.collect(key,
					new IntWritable(Integer.parseInt(value.toString())));
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

	public static class QuestionTagMapper extends MapReduceBase implements
			Mapper<LongWritable, Text, Text, Text> {

		public void map(LongWritable key, Text value,
				OutputCollector<Text, Text> output, Reporter reporter)
				throws IOException {

			// Input: XML String of the format <row key="value" />

			// Parse the input string into a nice map
			Map<String, String> parsed = Utils.transformXmlToMap(value
					.toString());

			String postTypeId = parsed.get("PostTypeId");
			String id = parsed.get("Id");
			String tags = parsed.get("Tags");

			if (postTypeId != null && postTypeId.equals("1")) {

				if (id != null && tags != null) {

					tags = Utils.cleanTags(tags);
					output.collect(new Text(id), new Text(tags));

				}
			}

			// Output: QuestionId, Tags Separated by Comma
		}

	}

	public static class UserTagReducer extends MapReduceBase implements
			Reducer<Text, Text, Text, Text> {

		private BufferedReader bufferedReader;
		private HashMap<String, HashSet<String>> questionUser = new HashMap<String, HashSet<String>>();

		public void configure(JobConf job) {
			String filePath = job.get("path");
			System.out.println("Path: " + filePath);
			loadUserQuestions(new Path(filePath + "userQuestion"));

		}

		private void loadUserQuestions(Path cachePath) {
			System.out.println("Loading Question/User HashMap..");

			try {
				FileSystem fs = FileSystem.get(new Configuration());

				FileStatus[] files = fs.listStatus(cachePath);
				for (FileStatus f : files) {
					// If that is a temp file, ignore it.
					if (new File(f.getPath().toString()).getName().startsWith(
							"_"))
						continue;
					String strLineRead = "";

					try {
						bufferedReader = new BufferedReader(
								new InputStreamReader(fs.open(f.getPath())));

						while ((strLineRead = bufferedReader.readLine()) != null) {
							String queUserArray[] = strLineRead.toString()
									.split("\\s+");
							String user = queUserArray[0];
							String question = queUserArray[1];

							HashSet<String> userHs;
							userHs = questionUser.get(question);
							if (userHs == null) {
								userHs = new HashSet<String>();
								userHs.add(user);
								questionUser.put(question, userHs);
							} else {
								userHs.add(user);
								questionUser.put(question, userHs);
							}
						}
					} catch (FileNotFoundException e) {
						e.printStackTrace();
					} catch (IOException e) {
						e.printStackTrace();
					} finally {
						if (bufferedReader != null) {
							bufferedReader.close();

						}

					}
				}

				// Utils.printHashMap(questionUser);

			} catch (Exception e) {
				e.printStackTrace();
				System.out.println("SWERR: File error.");
			}
		}

		public void reduce(Text key, Iterator<Text> values,
				OutputCollector<Text, Text> output, Reporter reporter)
				throws IOException {

			// Input: QuestionID, List of Strings (Tags seperated by comma)
			// System.out.println("Key: " + key.toString());
			HashSet<String> userHs = questionUser.get(key.toString());
			if (userHs != null) {
				while (values.hasNext()) {
					String value = values.next().toString();
					// System.out.println("Value: " + value);
					String[] tags = value.split(",");
					for (String tag : tags) {
						for (String user : userHs) {
							output.collect(new Text(user), new Text(tag));
						}
					}
				}
			} else {
				System.out
						.println("SWERR: QuestionID not found in Hash which is not possible.");
			}

			// Output: UserID, Tag
		}
	}

}
