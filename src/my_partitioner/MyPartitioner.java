/**
 * Copyright (C), 2015-2018, XXX有限公司
 * FileName: MyPatitioner
 * Author:   lvshuo
 * Date:     06/04/2018 11:29 AM
 * Description:
 * History:
 * <author>          <time>          <version>          <desc>
 * 作者姓名           修改时间           版本号              描述
 */
package my_partitioner;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

import java.io.IOException;

/**
 * 〈一句话功能简述〉<br> 
 * 〈〉
 *
 * @author lvshuo
 * @create 06/04/2018
 * @since 1.0.0
 */
public class MyPartitioner {

    public static class MyPartitionerMapper extends Mapper<LongWritable, Text, Text, Text>{

        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String [] arr_value = value.toString().split(" ");
            if (arr_value.length > 3){
                context.write(new Text("long"), value);
            } else if (arr_value.length < 3){
                context.write(new Text("short"), value);
            } else {
                context.write(new Text("right"), value);
            }
        }
    }

    public static class MyPartitionerPartitioner extends Partitioner<Text, Text>{

        @Override
        public int getPartition(Text key, Text value, int numPartitions) {
            int result = 0;
            if (key.toString().equals("long")){
                result = 0 % numPartitions;
            } else if (key.toString().equals("short")){
                result = 1 % numPartitions;
            } else{
                result = 2 % numPartitions;
            }
            return result;
        }
    }

    public static class MyPartitionerReducer extends Reducer<Text, Text, Text, Text>{

        @Override
        protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            // just output data from map
            for (Text val: values){
                context.write(key, val);
            }
        }
    }

    public static void main(String[] args) throws Exception{
        if (args.length != 2){
            System.err.println("Input: Output <input path> <output path>");
            System.exit(-1);
        }

        FileUtil.deleteDir("output");
        Configuration conf = new Configuration();

        Job job = new Job(conf, "my partitioner");
        job.setJarByClass(MyPartitioner.class);
        job.setNumReduceTasks(3);

        job.setMapperClass(MyPartitioner.MyPartitionerMapper.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);

        job.setPartitionerClass(MyPartitioner.MyPartitionerPartitioner.class);

        job.setReducerClass(MyPartitioner.MyPartitionerReducer.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);

        FileInputFormat.setInputPaths(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        boolean success = job.waitForCompletion(true);
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}