/**
 * Copyright (C), 2015-2018, XXX有限公司
 * FileName: MaxValue
 * Author:   lvshuo
 * Date:     03/04/2018 2:21 PM
 * Description:
 * History:
 * <author>          <time>          <version>          <desc>
 * 作者姓名           修改时间           版本号              描述
 */
package max_value;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
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
 * @create 03/04/2018
 * @since 1.0.0
 */
public class MaxValue {

    public static class MaxValueMapper extends Mapper<LongWritable, Text, Text, LongWritable>{

        private long maxNum = Long.MIN_VALUE;

        @Override
        public void map(LongWritable key, Text value, Context context)
                throws IOException, InterruptedException {

            String [] str = value.toString().split(" ");
            try{
                for (int i=0; i < str.length; i++){
                    int tmp = Integer.parseInt(str[i]);
                    if (tmp > maxNum){
                        maxNum = tmp;
                    }
                }
            }catch (NumberFormatException e){

            }
        }

        @Override
        protected void cleanup(Context context) throws IOException, InterruptedException {
            context.write(new Text("Max"), new LongWritable(maxNum));
        }
    }

    public static class MaxValueReducer extends Reducer<Text, LongWritable, Text, LongWritable>{

        private long maxNum = Long.MIN_VALUE;
        private Text one = new Text();

        @Override
        public void reduce(Text key, Iterable<LongWritable> values, Context context)
                throws IOException, InterruptedException {

            for (LongWritable val: values){
                if (val.get() > maxNum){
                    maxNum = val.get();
                }
            }
            one = key;
        }

        @Override
        protected void cleanup(Context context) throws IOException, InterruptedException {
            context.write(one, new LongWritable(maxNum));
        }
    }

    public static void main(String[] args) throws Exception{

        if (args.length != 2){
            System.err.println("Input: Output <input path> <output path>");
            System.exit(-1);
        }

        FileUtil.deleteDir("output");
        Configuration conf = new Configuration();

        Job job = new Job(conf, "find max value");
        job.setJarByClass(MaxValue.class);
        job.setMapperClass(MaxValue.MaxValueMapper.class);
        job.setCombinerClass(MaxValue.MaxValueReducer.class);
        job.setReducerClass(MaxValue.MaxValueReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(LongWritable.class);

        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);

        FileInputFormat.setInputPaths(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        boolean success = job.waitForCompletion(true);
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}