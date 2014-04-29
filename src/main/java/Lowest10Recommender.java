import java.io.BufferedReader;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.TreeMap;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;


public class Lowest10Recommender {
	public static class SOTLowest10Mapper extends MapReduceBase implements
	Mapper<LongWritable, Text, Text, Text> {
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

public void map(LongWritable key, Text value, OutputCollector<Text, Text> out,
		Reporter reporter) throws IOException {
	String[] values = value.toString().split(",");
	HashSet<String> userHs = questionUser.get(values[1]);
	if (userHs != null) {
		if (userHs.contains(values[0])) {
			// do nothing
		} else {
			if(Double.parseDouble(values[2])!=1.0)
			{
				out.collect(new Text(values[0]), new Text(values[1]+","+values[2]));
			}
		}
	} else {
		System.out.println("HOOLAHOOP:Question not found in user map");
	}

}
}

public static class SOTLowest10Reducer extends MapReduceBase implements
	Reducer<Text, Text, Text, Text> {

public void reduce(Text key, Iterator<Text> values,
		OutputCollector<Text, Text> output, Reporter reporter)
		throws IOException {

	TreeMap<Double, String> repToRecordMap = new TreeMap<Double, String>();

	// Input: UserId, List<QuestionId, score>

	while (values.hasNext()) {

		String value = values.next().toString();

		String qVal[] = value.split(",");

		repToRecordMap.put(Double.parseDouble(qVal[1]), qVal[0]);

		if (repToRecordMap.size() > 10) {
			repToRecordMap.remove(repToRecordMap.lastKey());
		}
	}

	String recQ = "";
	for (String str : repToRecordMap.values()) {
		recQ = recQ + str + ",";
	}
	recQ = recQ.substring(0, recQ.lastIndexOf(","));

	output.collect(key, new Text(recQ));

	// Output: UserId, Top 10 comma separated Question IDs
}
}

}
