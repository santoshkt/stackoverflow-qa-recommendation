import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.KeyValueTextInputFormat;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.TextInputFormat;
import org.apache.hadoop.mapred.TextOutputFormat;
import org.apache.hadoop.mapred.jobcontrol.Job;
import org.apache.hadoop.mapred.jobcontrol.JobControl;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.Map;

public class NoOfUsers extends Configured implements Tool {

	public static class NoOfUserMapper extends MapReduceBase implements
			Mapper<LongWritable, Text, Text, IntWritable> {

		public void map(LongWritable key, Text value,
				OutputCollector<Text, IntWritable> output, Reporter reporter)
				throws IOException {

			// Input: XML String of the format <row key="value" />

			// Parse the input string into a nice map
			Map<String, String> parsed = Utils.transformXmlToMap(value
					.toString());

			// Grab the "Text" field, since that is what we are counting over
			String Id = parsed.get("Id");

			if (Id != null && Integer.parseInt(Id)>0) {

				//if (parentId != null && ownerUserId != null) {
					output.collect(new Text("Users"), new IntWritable(1));
				//}
			}

			// Output: Users, QuestionId
		}
	}
		
	public static class QuestionQuestionPairReducer extends MapReduceBase
			implements Reducer<Text, IntWritable, Text, IntWritable> {

		public void reduce(Text key, Iterator<IntWritable> values,
				OutputCollector<Text, IntWritable> output, Reporter reporter)
				throws IOException {
					int sum = 0;
					while (values.hasNext()) {
						sum += values.next().get();
					}
				output.collect(key, new IntWritable(sum));
		}
	}
		
	public int run(String[] args) throws Exception {
		System.out.println("NoOfUsers.....");
		JobConf conf = new JobConf(NoOfUsers.class);
		conf.setMapperClass(NoOfUserMapper.class);
		conf.setJarByClass(NoOfUsers.class);
		conf.setReducerClass(QuestionQuestionPairReducer.class);
		//conf.setNumReduceTasks(0);
		conf.setMapOutputKeyClass(Text.class);
		conf.setMapOutputValueClass(IntWritable.class);

		conf.setOutputKeyClass(Text.class);
		conf.setOutputValueClass(IntWritable.class);

		conf.setInputFormat(TextInputFormat.class);
		conf.setOutputFormat(TextOutputFormat.class);

		FileInputFormat.addInputPath(conf, new Path(args[0]));
		FileOutputFormat.setOutputPath(conf, new Path(args[1]));

		Job job = new Job(conf);

		JobControl jobControl = new JobControl("jobControl");
		jobControl.addJob(job);
		try {
			handleRun(jobControl);
		} catch (InterruptedException e) {
			e.printStackTrace();
		}
	return 0;
  }
	public static class JobRunner implements Runnable {
		private JobControl control;
		public JobRunner(JobControl _control) {
			this.control = _control;
		}
		public void run() {
			this.control.run();
		}
	}
	public static void handleRun(JobControl control)
			throws InterruptedException {
		JobRunner runner = new JobRunner(control);
		Thread t = new Thread(runner);
		t.start();
		int i = 0;
		while (!control.allFinished()) {
			if (i % 20 == 0) {
				System.out
						.println(": Still running...");
				System.out.println("Running jobs: "
						+ control.getRunningJobs().toString());
				System.out.println("Waiting jobs: "
						+ control.getWaitingJobs().toString());
				System.out.println("Successful jobs: "
						+ control.getSuccessfulJobs().toString());
			}
			Thread.sleep(1000);
			i++;
		}
		if (control.getFailedJobs() != null) {
			System.out.println("Failed jobs: "
					+ control.getFailedJobs().toString());
		}
	}

  public static void main(String[] args) throws Exception {
    int res = ToolRunner.run(new Configuration(), new NoOfUsers(), args);
    System.exit(res);
  }

}
