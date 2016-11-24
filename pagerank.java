//SREELAKSHMI SETTURU
//ssetturu@uncc.edu
package assignment3;

import java.io.IOException;
import java.util.ArrayList;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configurable;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.log4j.Logger;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;

public class pagerank extends Configured implements Tool {

	private static final Logger LOG = Logger.getLogger(pagerank.class);

	public static void main(String[] args) throws Exception {
		int res = ToolRunner.run(new pagerank(), args);
		System.exit(res);
	}

	private static int count = 0;

	public int run(String[] args) throws Exception {
		
		String temp = "/user/ssetturu/op1";
		String tempout = args[1];
		// the job to get count of number of lines using "counters". It has only
		// one mapper and zero reducers.
		Job countjob = Job.getInstance(getConf(), " pagerank ");
		countjob.setJarByClass(this.getClass());
		FileInputFormat.addInputPaths(countjob, args[0]);
		FileOutputFormat.setOutputPath(countjob, new Path(temp));
		countjob.setMapperClass(countMap.class);
		countjob.setNumReduceTasks(0);
		countjob.setOutputKeyClass(Text.class);
		countjob.setOutputValueClass(Text.class);
		int success = countjob.waitForCompletion(true) ? 0 : 1;
		Long counter = countjob.getCounters().findCounter("Count", "Count")
				.getValue();
		System.out.println("long " + counter);
		// setting the count value in configuration, so that it can be
		// accessible to all jobs.
		getConf().set("count", counter + "");
		FileSystem fs1 = FileSystem.get(getConf());
		if (fs1.exists(new Path(temp))) {
			fs1.delete(new Path(temp), true);
		}
		// job to create link graph
		Job linkjob = Job.getInstance(getConf(), " pagerank ");
		linkjob.setJarByClass(this.getClass());
		FileInputFormat.addInputPaths(linkjob, args[0]);
		FileOutputFormat.setOutputPath(linkjob, new Path(temp));
		linkjob.setMapperClass(Map.class);
		linkjob.setReducerClass(Reduce.class);
		linkjob.setOutputKeyClass(Text.class);
		linkjob.setOutputValueClass(Text.class);
		success = linkjob.waitForCompletion(true) ? 0 : 1;
		System.out.println("first job status " + success);
		// iterating the job for ten times to compute pageranks
		if (success == 0) {
			for (int j = 0; j < 10; j++) {
				if (success == 0) {
					System.out.println("iteration number " + j);
					Job job1 = Job.getInstance(getConf(), " pagerank ");
					job1.setJarByClass(this.getClass());
					if (j == 0 || j % 2 == 0) {
						FileInputFormat.addInputPaths(job1, temp);
						FileOutputFormat.setOutputPath(job1, new Path(tempout));
					} else {
						FileInputFormat.addInputPaths(job1, tempout);
						FileOutputFormat.setOutputPath(job1, new Path(temp));

					}
					job1.setMapperClass(Map1.class);
					job1.setReducerClass(Reduce1.class);
					job1.setOutputKeyClass(Text.class);
					job1.setOutputValueClass(Text.class);
					success = job1.waitForCompletion(true) ? 0 : 1;
					FileSystem fs = FileSystem.get(getConf());
					if (j == 0 || j % 2 == 0) {
						if (fs.exists(new Path(temp))) {
							fs.delete(new Path(temp), true);
						}
					} else {
						if (fs.exists(new Path(tempout))) {
							fs.delete(new Path(tempout), true);
						}

					}
				}
			}
		}
		if (success == 0) {
			Job job2 = Job.getInstance(getConf(), " pagerank ");
			job2.setJarByClass(this.getClass());
			FileInputFormat.addInputPaths(job2, temp);
			FileOutputFormat.setOutputPath(job2, new Path(tempout));
			job2.setNumReduceTasks(1);
			job2.setMapperClass(Map2.class);
			job2.setReducerClass(Reduce2.class);
			job2.setMapOutputKeyClass(DoubleWritable.class);
			job2.setMapOutputValueClass(Text.class);
			job2.setOutputKeyClass(Text.class);
			job2.setOutputValueClass(DoubleWritable.class);
			success = job2.waitForCompletion(true) ? 0 : 1;
			FileSystem fs = FileSystem.get(getConf());
			if (fs.exists(new Path(temp))) {
				fs.delete(new Path(temp), true);
			}
		}
		return success;
	}

	// Just one mapper to get count of number of lines of the input file.
	public static class countMap extends Mapper<LongWritable, Text, Text, Text> {
		public void map(LongWritable offset, Text lineText, Context context)
				throws IOException, InterruptedException {
			context.getCounter("Count", "Count").increment(1);
			context.write(new Text("count"), new Text("1"));
		}
	}

	public static class Map extends Mapper<LongWritable, Text, Text, Text> {
		public void map(LongWritable offset, Text lineText, Context context)
				throws IOException, InterruptedException {
			String line = lineText.toString();
			// extracting the title of the document
			final Pattern pattern = Pattern.compile("<title>(.+?)</title>");
			final Matcher matcher = pattern.matcher(line);
			matcher.find();
			String title = matcher.group(1);
			// extracting outlinks only if they are in <text> tags. otherwise
			// they are comments which can be ignored.
			final Pattern txt = Pattern.compile("<text xml(.*?)</text>");
			final Matcher m = txt.matcher(line);
			if (m.find()) {
				String text = m.group();
				// extracting the outlinks of the document
				final Pattern pattern1 = Pattern.compile("\\[\\[(.*?)\\]\\]");
				final Matcher matcher1 = pattern1.matcher(text);
				final Pattern pattern2 = Pattern.compile("\\[\\[(.*?)\\]\\]");
				final Matcher matcher2 = pattern2.matcher(text);
				if (matcher2.find()) {
					while (matcher1.find()) {
						String outlink = matcher1.group(1);
						// emiting title and outlinks of the document as key
						// value pairs
						context.write(new Text(title), new Text(outlink));
					}
				} else {
					// if there are no ooutlinks for a document then value will
					// be a tab.
					context.write(new Text(title), new Text("\t"));

				}
			} else {
				// if there is no text tag in a line implies there are no
				// outlinks for that document, so again the value is a tab.
				context.write(new Text(title), new Text("\t"));

			}
		}
	}

	public static class Reduce extends Reducer<Text, Text, Text, Text> {

		@Override
		public void reduce(Text word, Iterable<Text> counts, Context context)
				throws IOException, InterruptedException {
			// getting the count of lines value
			String cc = context.getConfiguration().get("count");
			double c = Double.parseDouble(cc);
			// initializing page rank to 1/number of lines
			double pg = 1 / c;
			String key = word.toString() + "@@#=@@#" + Double.toString(pg)
					+ "@@@###@@@###";
			StringBuilder sb = new StringBuilder();
			for (Text count : counts) {
				if (count.toString().equals("\t")) {
					sb.append("\t");
				} else {
					sb.append(count.toString());
					sb.append("@$@$##");
				}
			}
			// constructing link graph
			// key is title of the document with its pagerank, value is outlink
			// information
			context.write(new Text(key), new Text(sb.toString()));

		}
	}

	public static class Map1 extends Mapper<LongWritable, Text, Text, Text> {
		public void map(LongWritable offset, Text lineText, Context context)
				throws IOException, InterruptedException {
			String line = lineText.toString();
			// extractng the title
			String title = line.split("@@#=@@#")[0];
			double pagerank = 0.0;
			// counting number of outlinks to calculate pagerank contibution of
			// a document to its outlinks
			int outlinks = StringUtils.countMatches(line, "@$@$##");
			final Pattern pgr = Pattern.compile("@@#=@@#(.+?)@@@###@@@###\t");
			final Matcher matcher1 = pgr.matcher(line);
			matcher1.find();
			String prank = matcher1.group(1);
			if (outlinks == 0) {
				pagerank = Double.parseDouble(prank);
			} else {
				pagerank = Double.parseDouble(prank) / outlinks;
			}
			// splitting the line from linkgraph to extract outlink information
			String[] temp = line.split("@@@###@@@###\t");
			// checking if it has outlinks then the split should give
			if (temp.length >= 2) {
				String[] temp1 = temp[1].split("@\\$@\\$##");
				for (int i = 0; i < temp1.length; i++) {
					// here key is each outlink of the given document and value
					// is page rank calculated above.
					context.write(new Text(temp1[i]),
							new Text(Double.toString(pagerank)));
				}
				if (temp[1] != "\t") {
					// if the document has outlinks, passing title as keys and
					// outlink information as value. this is used to reconstruct
					// the link graph in reducer.
					context.write(new Text(title), new Text(temp[1]));
				}
			} else {
				context.write(new Text(title), new Text("\t"));

			}
		}
	}

	public static class Reduce1 extends Reducer<Text, Text, Text, Text> {

		@Override
		public void reduce(Text word, Iterable<Text> values, Context context)
				throws IOException, InterruptedException {
			Double sumpg = 0.0, d = 0.85;
			String out = "", ss, check = "#";
			// if the values are pagerank, summing up the pagerank values from
			// all its inlinks to calculate actual pagerank. if the value is a
			// string, then it is keys outlinks information. So appending it and
			// skipping from summation.
			try {
				for (Text value : values) {
					ss = value.toString();
					if (ss.contains("@$@$##")) {
						out = out + value.toString();
						check = "$";
					} else if (ss.equals("\t")) {
						out = "\t";
						check = "$";
					} else {
						sumpg = sumpg + Double.parseDouble(ss);
					}
				}
			} catch (NumberFormatException e) {
				System.out.println("not a number");
			} finally {
				sumpg = (1 - d) + d * sumpg;
				String key = word.toString() + "@@#=@@#"
						+ Double.toString(sumpg) + "@@@###@@@###";
				// reconstructing the link graph by passing title,it page rank
				// as key and outlinks information as value. Also filtering the
				// keys based on a condition so as pageranks of only titles are
				// being written to output file.
				if (check.equals("$")) {
					context.write(new Text(key), new Text(out));
				}
			}
		}
	}

	public static class Map2 extends
			Mapper<LongWritable, Text, DoubleWritable, Text> {
		public void map(LongWritable offset, Text lineText, Context context)
				throws IOException, InterruptedException {
			// Extracting title and pagerank and passing negative of pagerank as
			// keys so as to get pageranks sorted as desired and titles as
			// values.
			String line = lineText.toString();
			String title = line.split("@@#=@@#")[0];
			final Pattern pgr = Pattern.compile("@@#=@@#(.+?)@@@###@@@###\t");
			final Matcher matcher2 = pgr.matcher(line);
			matcher2.find();
			String pgrnk = matcher2.group(1);
			double pg = Double.parseDouble(pgrnk);
			context.write(new DoubleWritable(-pg), new Text(title));
		}
	}

	public static class Reduce2 extends
			Reducer<DoubleWritable, Text, Text, DoubleWritable> {
		@Override
		public void reduce(DoubleWritable value, Iterable<Text> words,
				Context context) throws IOException, InterruptedException {
			// titles as keys and pageranks as values. the output will be in
			// sorted order(descending)
			double pgr = value.get();
			for (Text values : words) {
				context.write(values, new DoubleWritable(-pgr));
			}

		}

	}

}
