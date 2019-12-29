import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.time.LocalDateTime;

public class BikeDriver extends Configured implements Tool {
	private static String hdfsuri = "hdfs://quickstart.cloudera:8020";

	@Override
	public int run(String[] args) throws Exception {
		String input, output;
		if (args.length == 2) {
			input = args[0];
			output = args[1];
		} else {
			System.err.println("Incorrect number of arguments.  Expected: input output");
			return -1;
		}

		deleteLocalDir(output);

		Configuration conf = getConf();
		conf.set("mapred.textoutputformat.separator", ",");

		Job job = new Job(getConf());
		job.setJarByClass(BikeDriver.class);
		job.setJobName(this.getClass().getName());

		FileInputFormat.setInputPaths(job, new Path(input));
		FileOutputFormat.setOutputPath(job, new Path(output));

		job.setMapperClass(BikeMapper.class);
		job.setReducerClass(BikeReducer.class);

		job.setMapOutputKeyClass(LongWritable.class);
		job.setMapOutputValueClass(Text.class);

		job.setOutputKeyClass(LongWritable.class);
		job.setOutputValueClass(Text.class);

		boolean success = job.waitForCompletion(true);
		return success ? 0 : 1;
	}

	public static void main(String[] args) throws Exception {
		BikeDriver driver = new BikeDriver();
//		Hdfs hdfs = new Hdfs();
//		hdfs.confHdfs();
//		LocalDateTime now = LocalDateTime.now();
//		int year = now.getYear();
//		int month = now.getMonthValue();
//		int day = now.getDayOfMonth();


//		String path = "/user/hdfs/example/hdfs/DataSource/";
//		String pathAnalysis1 = "/user/hdfs/example/hdfs/Analysis1/";
//		String pathAnalysis2 = "/user/hdfs/example/hdfs/Analysis2/";
//		String lastFile = hdfs.getLastFileName(path);

//		System.out.println("Analysis file: " + lastFile);

//		String[] listOfArguments = { hdfsuri + path + lastFile, "output" , "output2"};

		long timeStart = System.currentTimeMillis();
		int exitCode = ToolRunner.run(driver, args);
		long timeStop = System.currentTimeMillis();
//		String logsFileName = "hadoop.log." + year + "-" + month + "-" + day;
//		System.out.println(logsFileName);
//		String fileNameToSave = "results_middleSet.txt";
//		System.out.println(fileNameToSave);

//		removeBlankLineFromTheEnd("./output/part-r-00000");
//		removeBlankLineFromTheEnd("./output2/part-r-00000");

		// copy file from output catalog to hdfs (maybe name should be created based on current date)
//		hdfs.copyFileFromLocal("output/part-r-00000", pathAnalysis1 + fileNameToSave);
//		hdfs.copyFileFromLocal("output2/part-r-00000", pathAnalysis2 + fileNameToSave);
//		hdfs.setPermission(pathAnalysis1 + fileNameToSave);
//		hdfs.setPermission(pathAnalysis2 + fileNameToSave);
//		hdfs.copyFileFromLocal("hadoop.log", "/user/hdfs/example/hdfs/Logs/" + logsFileName);
		System.out.println("Time: " + (timeStop - timeStart)/1000 + "s");

		System.exit(exitCode);
	}

	private void deleteLocalDir(String output) {
		File dir = new File(output);

		File[] listFiles = dir.listFiles();
		if (listFiles != null) {
			for (File file : listFiles) {
				System.out.println("Deleting " + file.getName());
				file.delete();
			}
			System.out.println("Deleting Directory. Success = " + dir.delete());
		}
	}

	private static void removeBlankLineFromTheEnd(String filePath)
			throws IOException {
		RandomAccessFile f = new RandomAccessFile(filePath, "rw");
		long length = f.length();
		byte b;
		if (length != 0) {
			do {
				length -= 1;
				f.seek(length);
				b = f.readByte();
			} while(b != 10);
			f.setLength(length);
			f.close();
		}
	}
}