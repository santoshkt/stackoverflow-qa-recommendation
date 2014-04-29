import java.io.IOException;
import java.util.Date;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.KeyValueTextInputFormat;
import org.apache.hadoop.mapred.TextInputFormat;
import org.apache.hadoop.mapred.TextOutputFormat;
import org.apache.hadoop.mapred.jobcontrol.Job;
import org.apache.hadoop.mapred.jobcontrol.JobControl;
import org.apache.hadoop.mapred.lib.IdentityMapper;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class SOTagDriver extends Configured implements Tool {

	public static long starttime;
	public static long endtime;

	public static void startTimer() {
		starttime = System.currentTimeMillis();
	}

	public static void stopTimer() {
		endtime = System.currentTimeMillis();
	}

	public static float getJobTimeInSecs() {
		return (endtime - starttime) / (float) 1000;
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
						.println(new Date().toString() + ": Still running...");
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

	private void questionTagGenerator(String[] args) throws IOException {
		System.out.println("Question Tag generation..");
		JobConf conf = new JobConf(SOTagDriver.class);
		conf.setMapperClass(TagPreProcessing.QuestionTagsMapper.class);
		conf.setReducerClass(TagPreProcessing.QuestionTagsReducer.class);
		conf.setJarByClass(SOTagDriver.class);

		conf.setMapOutputKeyClass(Text.class);
		conf.setMapOutputValueClass(Text.class);

		conf.setOutputKeyClass(Text.class);
		conf.setOutputValueClass(Text.class);

		conf.setInputFormat(TextInputFormat.class);
		conf.setOutputFormat(TextOutputFormat.class);

		FileInputFormat.addInputPath(conf, new Path(args[0] + args[1]));
		FileOutputFormat.setOutputPath(conf, new Path(args[0]
				+ "generatedQuestionTags"));
		Job job = new Job(conf);

		JobControl jobControl = new JobControl("jobControl");
		jobControl.addJob(job);
		try {
			handleRun(jobControl);
		} catch (InterruptedException e) {
			e.printStackTrace();
		}
	}

	private void userTagGenerator(String[] args) throws IOException {
		System.out.println("User Tag generation..");
		JobConf conf = new JobConf(SOTagDriver.class);
		conf.setMapperClass(IdentityMapper.class);
		conf.setReducerClass(TagPreProcessing.UserTagsReducer.class);
		conf.setJarByClass(SOTagDriver.class);

		conf.setMapOutputKeyClass(Text.class);
		conf.setMapOutputValueClass(Text.class);

		conf.setOutputKeyClass(Text.class);
		conf.setOutputValueClass(Text.class);

		conf.setInputFormat(KeyValueTextInputFormat.class);
		conf.setOutputFormat(TextOutputFormat.class);

		FileInputFormat.addInputPath(conf, new Path(args[0] + "userTag"));
		FileOutputFormat.setOutputPath(conf, new Path(args[0]
				+ "generatedUserTags"));
		Job job = new Job(conf);

		JobControl jobControl = new JobControl("jobControl");
		jobControl.addJob(job);
		try {
			handleRun(jobControl);
		} catch (InterruptedException e) {
			e.printStackTrace();
		}
	}

	private void findSimilarity(String[] args) throws IOException {
		System.out.println("Finding similarity..");
		JobConf conf = new JobConf(SOTagDriver.class);
		conf.setMapperClass(TagSimilarity.TagSimilarityMapper.class);
		conf.setReducerClass(TagSimilarity.TagSimilarityReducer.class);
		conf.setJarByClass(SOTagDriver.class);

		conf.setMapOutputKeyClass(LongWritable.class);
		conf.setMapOutputValueClass(Text.class);

		conf.setOutputKeyClass(NullWritable.class);
		conf.setOutputValueClass(Text.class);

		conf.setInputFormat(KeyValueTextInputFormat.class);
		conf.setOutputFormat(TextOutputFormat.class);

		FileInputFormat.addInputPath(conf, new Path(args[0]
				+ "generatedUserTags"));
		FileInputFormat.addInputPath(conf, new Path(args[0]
				+ "generatedQuestionTags"));
		FileOutputFormat.setOutputPath(conf, new Path(args[0] + args[2]));

		conf.set("bucketCount", "1000");
		conf.setOutputValueGroupingComparator(TagSimilarity.IdPairGroupComprator.class);
		conf.setPartitionerClass(TagSimilarity.IdPairPartitioner.class);

		Job job = new Job(conf);

		JobControl jobControl = new JobControl("jobControl");
		jobControl.addJob(job);
		try {
			handleRun(jobControl);
		} catch (InterruptedException e) {
			e.printStackTrace();
		}
	}

	private void printline() {
		System.out.print("\n");
		for (int i = 0; i < 80; i++) {
			System.out.print("=");
		}
		System.out.print("\n");
		System.out.flush();
	}

	public int run(String[] args) throws Exception {

		startTimer();
		//questionTagGenerator(args);
		stopTimer();
		printline();
		System.out.println("Total time for question Tag generation data: "
				+ getJobTimeInSecs() + "seconds");
		printline();
		startTimer();
		//userTagGenerator(args);
		stopTimer();
		printline();
		System.out.println("Total time for user Tag Generation: "
				+ getJobTimeInSecs() + "seconds");
		printline();
		startTimer();
		findSimilarity(args);
		stopTimer();
		printline();
		System.out.println("Total time for finding similarity: "
				+ getJobTimeInSecs() + "seconds");
		return 0;
	}

	public static void main(String args[]) throws Exception {

		System.out.println("Program started");
		if (args.length != 3) {
			System.err.println("Usage: SOTagDriver <path> <input> <output>");
			System.exit(-1);
		}

		Configuration configuration = new Configuration();
		String[] otherArgs = new GenericOptionsParser(configuration, args)
				.getRemainingArgs();
		ToolRunner.run(new SOTagDriver(), otherArgs);
		System.out.println("Program complete.");
		System.exit(0);
	}
}
