import java.io.IOException;
import java.net.URISyntaxException;
import java.util.Date;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.KeyValueTextInputFormat;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.TextInputFormat;
import org.apache.hadoop.mapred.TextOutputFormat;
import org.apache.hadoop.mapred.jobcontrol.Job;
import org.apache.hadoop.mapred.jobcontrol.JobControl;
import org.apache.hadoop.mapred.lib.IdentityMapper;
import org.apache.hadoop.mapred.lib.aggregate.LongValueSum;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class SODriver extends Configured implements Tool {

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

	private void preprocessData(String[] args) throws IOException {
		System.out.println("Preprocessing..");
		JobConf conf = new JobConf(SODriver.class);
		conf.setMapperClass(PreProcessing.UserQuestionPairMapper.class);
		conf.setReducerClass(PreProcessing.QuestionQuestionPairReducer.class);
		conf.setJarByClass(SODriver.class);

		conf.setMapOutputKeyClass(Text.class);
		conf.setMapOutputValueClass(Text.class);

		conf.setOutputKeyClass(Text.class);
		conf.setOutputValueClass(IntWritable.class);

		conf.setInputFormat(TextInputFormat.class);
		conf.setOutputFormat(TextOutputFormat.class);

		FileInputFormat.addInputPath(conf, new Path(args[0] + args[1]));
		FileOutputFormat.setOutputPath(conf, new Path(args[0]
				+ "questionsCooccurenceTemp"));

		Job job = new Job(conf);

		JobControl jobControl = new JobControl("jobControl");
		jobControl.addJob(job);
		try {
			handleRun(jobControl);
		} catch (InterruptedException e) {
			e.printStackTrace();
		}
	}

	private void cooccurence(String[] args) throws IOException {
		System.out.println("Co-occurrence..");
		JobConf conf = new JobConf(SODriver.class);
		conf.setMapperClass(PreProcessing.QuestionQuestionSumMapper.class);
		conf.setReducerClass(PreProcessing.QuestionQuestionSumReducer.class);
		conf.setJarByClass(SODriver.class);

		conf.setMapOutputKeyClass(Text.class);
		conf.setMapOutputValueClass(IntWritable.class);

		conf.setOutputKeyClass(Text.class);
		conf.setOutputValueClass(IntWritable.class);

		conf.setInputFormat(KeyValueTextInputFormat.class);
		conf.setOutputFormat(TextOutputFormat.class);

		FileInputFormat.addInputPath(conf, new Path(args[0]
				+ "questionsCooccurenceTemp"));
		FileOutputFormat.setOutputPath(conf, new Path(args[0]
				+ "questionsCooccurence"));

		Job job = new Job(conf);

		JobControl jobControl = new JobControl("jobControl");
		jobControl.addJob(job);
		try {
			handleRun(jobControl);
		} catch (InterruptedException e) {
			e.printStackTrace();
		}
	}

	private void userQuestionPairs(String args[]) throws IOException {
		System.out.println("userQuestion pairs..");
		JobConf conf = new JobConf(SODriver.class);
		conf.setMapperClass(PreProcessing.UserQuestionPairMapper.class);
		conf.setReducerClass(Reducer.class);
		conf.setNumReduceTasks(0);
		conf.setJarByClass(SODriver.class);

		conf.setMapOutputKeyClass(Text.class);
		conf.setMapOutputValueClass(Text.class);

		conf.setOutputKeyClass(Text.class);
		conf.setOutputValueClass(Text.class);

		conf.setInputFormat(TextInputFormat.class);
		conf.setOutputFormat(TextOutputFormat.class);

		FileInputFormat.addInputPath(conf, new Path(args[0] + args[1]));
		FileOutputFormat
				.setOutputPath(conf, new Path(args[0] + "userQuestion"));

		Job job = new Job(conf);

		JobControl jobControl = new JobControl("jobControl");
		jobControl.addJob(job);
		try {
			handleRun(jobControl);
		} catch (InterruptedException e) {
			e.printStackTrace();
		}
	}

	private static void userTagPairs(String args[]) throws IOException,
			URISyntaxException {
		System.out.println("User Tag pairs..");
		JobConf conf = new JobConf(SODriver.class);
		conf.setMapperClass(PreProcessing.QuestionTagMapper.class);
		conf.setReducerClass(PreProcessing.UserTagReducer.class);
		conf.setJarByClass(SODriver.class);

		conf.setMapOutputKeyClass(Text.class);
		conf.setMapOutputValueClass(Text.class);

		conf.setOutputKeyClass(Text.class);
		conf.setOutputValueClass(Text.class);

		conf.setInputFormat(TextInputFormat.class);
		conf.setOutputFormat(TextOutputFormat.class);

		FileInputFormat.addInputPath(conf, new Path(args[0] + args[1]));
		FileOutputFormat.setOutputPath(conf, new Path(args[0] + "userTag"));
		conf.set("path", args[0]);

		Job job = new Job(conf);

		JobControl jobControl = new JobControl("jobControl");
		jobControl.addJob(job);
		try {
			handleRun(jobControl);
		} catch (InterruptedException e) {
			e.printStackTrace();
		}
	}

	private void matrixMultiplicationPairs(String[] args) throws IOException {
		System.out.println("matrix multiplication pairs..");
		JobConf conf = new JobConf(SODriver.class);
		conf.setMapperClass(MatrixMultiplication.MultiplicationPairsMapper.class);
		conf.setReducerClass(MatrixMultiplication.MultiplicationPairsReducer.class);
		conf.setJarByClass(SODriver.class);

		conf.setMapOutputKeyClass(Text.class);
		conf.setMapOutputValueClass(Text.class);

		conf.setOutputKeyClass(Text.class);
		conf.setOutputValueClass(Text.class);

		conf.setInputFormat(KeyValueTextInputFormat.class);
		conf.setOutputFormat(TextOutputFormat.class);

		FileInputFormat.addInputPath(conf, new Path(args[0]
				+ "questionsCooccurence"));
		FileOutputFormat.setOutputPath(conf, new Path(args[0]
				+ "matrixProductOutput"));

		conf.set("path", args[0]);

		Job job = new Job(conf);

		JobControl jobControl = new JobControl("jobControl");
		jobControl.addJob(job);
		try {
			handleRun(jobControl);
		} catch (InterruptedException e) {
			e.printStackTrace();
		}
	}

	private static void top10questions(String args[]) throws IOException,
			URISyntaxException {
		System.out.println("Suggesting top 10 questions for each user..");
		JobConf conf = new JobConf(SODriver.class);
		conf.setMapperClass(IdentityMapper.class);
		conf.setReducerClass(Top10Recommender.SOTop10Reducer.class);
		conf.setJarByClass(SODriver.class);

		conf.setMapOutputKeyClass(Text.class);
		conf.setMapOutputValueClass(Text.class);

		conf.setOutputKeyClass(Text.class);
		conf.setOutputValueClass(Text.class);

		conf.setInputFormat(KeyValueTextInputFormat.class);
		conf.setOutputFormat(TextOutputFormat.class);

		FileInputFormat.addInputPath(conf, new Path(args[0]
				+ "matrixProductOutput"));
		FileOutputFormat.setOutputPath(conf, new Path(args[0] + args[2]));
		conf.set("path", args[0]);

		Job job = new Job(conf);

		JobControl jobControl = new JobControl("jobControl");
		jobControl.addJob(job);
		try {
			handleRun(jobControl);
		} catch (InterruptedException e) {
			e.printStackTrace();
		}
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

	private void printline() {
		System.out.print("\n");
		for (int i = 0; i < 80; i++) {
			System.out.print("=");
		}
		System.out.print("\n");
		System.out.flush();
	}

	public int run(String[] args) throws Exception {

		// 1. Pre-process the data.

		startTimer();
		preprocessData(args);
		stopTimer();
		printline();
		System.out.println("Total time for pre-processing data: "
				+ getJobTimeInSecs() + "seconds");
		printline();
		startTimer();
		cooccurence(args);
		stopTimer();
		printline();
		System.out.println("Total time for co-occurence: " + getJobTimeInSecs()
				+ "seconds");
		printline();
		startTimer();
		userQuestionPairs(args);
		stopTimer();
		printline();
		System.out.println("Total time for userQuestionPairs: "
				+ getJobTimeInSecs() + "seconds");
		printline();
		startTimer();
		userTagPairs(args);
		stopTimer();
		printline();
		System.out.println("Total time for userTagPairs: " + getJobTimeInSecs()
				+ "seconds");
		printline();

		// 2. Matrix multiplication of Questions Co-occurence matrix and User
		// Preference matrix

		startTimer();
		matrixMultiplicationPairs(args);
		stopTimer();
		printline();
		System.out.println("Total time for matrix product: "
				+ getJobTimeInSecs() + "seconds");

		// 3. Recommend top questions
		startTimer();
		top10questions(args);
		stopTimer();
		printline();
		System.out.println("Total time for Top 10 recommendation: "
				+ getJobTimeInSecs() + "seconds");
		printline();

		return 0;
	}

	public static void main(String args[]) throws Exception {

		System.out.println("Program started");
		if (args.length != 3) {
			System.err.println("Usage: SODriver <path> <input> <output>");
			System.exit(-1);
		}

		Configuration configuration = new Configuration();
		String[] otherArgs = new GenericOptionsParser(configuration, args)
				.getRemainingArgs();
		ToolRunner.run(new SODriver(), otherArgs);
		System.out.println("Program complete.");
		System.exit(0);
	}
}
