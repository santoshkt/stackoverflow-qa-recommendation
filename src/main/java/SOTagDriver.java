import java.util.Date;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.mapred.jobcontrol.JobControl;
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

	private void printline() {
		System.out.print("\n");
		for (int i = 0; i < 80; i++) {
			System.out.print("=");
		}
		System.out.print("\n");
		System.out.flush();
	}

	public int run(String[] args) throws Exception {

	
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
		ToolRunner.run(new SODriver(), otherArgs);
		System.out.println("Program complete.");
		System.exit(0);
	}
}
