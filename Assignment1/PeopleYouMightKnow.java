package edu.rutgers.cs550.peopleyoumightknow;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map.Entry;
import java.util.StringTokenizer;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class PeopleYouMightKnow extends Configured implements Tool {
	public static void main(String[] args) throws Exception {
		System.out.println(Arrays.toString(args));
		int res = ToolRunner.run(new Configuration(), new PeopleYouMightKnow(),
				args);

		System.exit(res);
	}

	@Override
	public int run(String[] args) throws Exception {
		System.out.println(Arrays.toString(args));
		Job job = new Job(getConf(), "PeopleYouMightKnow");
		job.setJarByClass(PeopleYouMightKnow.class);
		job.setOutputKeyClass(IntWritable.class);
		job.setOutputValueClass(Text.class);

		job.setMapperClass(Map.class);
		job.setReducerClass(Reduce.class);

		job.setInputFormatClass(TextInputFormat.class);
		job.setOutputFormatClass(TextOutputFormat.class);

		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));

		job.waitForCompletion(true);

		return 0;
	}

	public static class Map extends
			Mapper<LongWritable, Text, IntWritable, Text> {

		@Override
		public void map(LongWritable key, Text value, Context context)
				throws IOException, InterruptedException {
			String user[] = value.toString().split("\t");

			if (user[0].equals("")) {
				return;
			}

			IntWritable userKey = new IntWritable(Integer.parseInt(user[0]));
			List<String> friends = new ArrayList<String>();

			if (user.length == 2) {
				if (user[1].equals("")) {
					return;
				}

				StringTokenizer tokenizer = new StringTokenizer(user[1], ",");

				while (tokenizer.hasMoreTokens()) {
					String friend = tokenizer.nextToken();
					friends.add(friend);
					context.write(userKey, new Text("1," + friend));
				}

				for (int i = 0; i < friends.size(); i++) {
					for (int j = i + 1; j < friends.size(); j++) {
						context.write(
								new IntWritable(
										Integer.parseInt(friends.get(i))),
								new Text("2," + friends.get(j)));
						context.write(
								new IntWritable(
										Integer.parseInt(friends.get(j))),
								new Text("2," + friends.get(i)));
					}
				}
			}
		}
	}

	public static class Reduce extends
			Reducer<IntWritable, Text, IntWritable, Text> {

		@Override
		public void reduce(IntWritable key, Iterable<Text> values,
				Context context) throws IOException, InterruptedException {
			String[] mutualFriends;
			HashMap<String, Integer> map = new HashMap<String, Integer>();
			for (Text value : values) {
				mutualFriends = value.toString().split(",");
				if (mutualFriends[0].equals("1")) {
					map.put(mutualFriends[1], 0);
				} else if (mutualFriends[0].equals("2")) {
					if (map.containsKey(mutualFriends[1])) {
						if (map.get(mutualFriends[1]) != 0) {
							map.put(mutualFriends[1],
									map.get(mutualFriends[1]) + 1);
						}
					} else {
						map.put(mutualFriends[1], 1);
					}
				}
			}

			List<Entry<String, Integer>> list = new ArrayList<Entry<String, Integer>>(
					map.entrySet());
			Collections.sort(list, new Comparator<Entry<String, Integer>>() {
				public int compare(Entry<String, Integer> e1,
						Entry<String, Integer> e2) {
					if (e1.getValue() > e2.getValue())
						return -1;
					else if (e1.getValue() < e2.getValue())
						return 1;
					else {
						if (Integer.parseInt(e1.getKey()) > Integer.parseInt(e2
								.getKey()))
							return 1;

						else
							return -1;

					}
				}
			});

			List<String> result = new ArrayList<String>();

			for (int i = 0; i < Math.min(10, list.size()); i++) {
				if (!list.get(i).getValue().equals(0)) {
					result.add(list.get(i).getKey());
				}
			}
			context.write(key, new Text(StringUtils.join(result, ",")));
		}
	}
}