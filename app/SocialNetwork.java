import java.io.IOException;
import java.util.Map.Entry;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.DataInput;
import java.io.DataOutput;
import java.util.*;
import java.util.stream.*;

public class SocialNetwork {

	// Custom context value output from map operation
	public static class MutualFriend implements Writable {
		public Long mutualFriend;
		public Long user;

		/**
		 * Mutual friend between 2 users (otherUser and user used as a key in context)
		 * 
		 * Indicates mutualFriend is friends with otherUser and user in context
		 */
		public MutualFriend(Long mutualFriend, Long otherUser) {
			this.mutualFriend = mutualFriend;
			this.user = otherUser;
		}

		// Base methods required for Writable interface.
		public MutualFriend() {
			this.mutualFriend = -1L;
			this.user = -1L;
		}

		// Read from context
        @Override
        public void readFields(DataInput in) throws IOException {
            this.mutualFriend = in.readLong();
            this.user = in.readLong();
        }

		// Write output to context
        @Override
        public void write(DataOutput out) throws IOException {
            out.writeLong(this.mutualFriend);
            out.writeLong(this.user);
        }

        @Override
        public String toString() {
            return "";
        }
	}

	public static class TokenizerMapper extends Mapper<LongWritable, Text, LongWritable, MutualFriend>{

		/**
		 * For each user and for each friend of the user, we add mutual friends relationship between 
		 * all the other friends of the user by using the key as the friends' ids.
		 * 
		 * We also add mutual friend index -1 to indicate when a user is already friend's with another.
		 * Since map executes on each line separately, this is necessary since we only have context
		 * of each line at a time.
		 */
		public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
			String line[] = value.toString().split("\t");
			Long userId = Long.parseLong(line[0]);
			if (line.length < 2) {
				// In this case, the user has no friends, so we just output an empty
				// element to add one context at least
				context.write(new LongWritable(userId), new MutualFriend());
				return;
			}
			
			// Transform the comma separated lists into a list of longs.
			List<Long> friends = Arrays.stream(line[1].split(",")).mapToLong(Long::parseLong)
										.boxed()
										.collect(Collectors.toList());

			// Adding the mutual friends relationship to the context and already friends relationship
			for (int i = 0; i < friends.size(); i++) {
				for (int j = i; j < friends.size(); j++) {
					context.write(new LongWritable(friends.get(i)), new MutualFriend(userId, friends.get(j)));
					context.write(new LongWritable(friends.get(j)), new MutualFriend(userId, friends.get(i)));
				}
				context.write(new LongWritable(userId), new MutualFriend(-1L, friends.get(i)));
			}
		}

	}

	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();
		Job job = Job.getInstance(conf, "social network");
		job.setJarByClass(SocialNetwork.class);
		job.setMapperClass(TokenizerMapper.class);
		job.setReducerClass(Reduce.class);
		job.setOutputKeyClass(LongWritable.class);
		job.setOutputValueClass(MutualFriend.class);

		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}
}
