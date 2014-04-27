import java.io.BufferedReader;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;

public class MatrixMultiplication {

	public static class MultiplicationPairsMapper extends MapReduceBase
			implements Mapper<Text, Text, Text, Text> {

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

		public void map(Text key, Text value, OutputCollector<Text, Text> out,
				Reporter reporter) throws IOException {
			String[] values = key.toString().split(",");
			HashSet<String> userHs = questionUser.get(values[1]);
			if (userHs != null) {
				for (String user : userHs) {
					out.collect(new Text(user + "," + values[0]), value);
				}
			} else {
				System.out.println("HOOLAHOOP:Question not found in user map");
			}

		}
	}

	public static class MultiplicationPairsReducer extends MapReduceBase
			implements Reducer<Text, Text, Text, Text> {

		public void reduce(Text key, Iterator<Text> values,
				OutputCollector<Text, Text> out, Reporter reporter)
				throws IOException {
			Integer sum = 0;
			while (values.hasNext()) {
				sum = sum + Integer.parseInt(values.next().toString());
			}
			String[] val = key.toString().split(",");
			out.collect(new Text(val[0]),
					new Text(val[1] + "," + sum.toString()));
		}

	}
}