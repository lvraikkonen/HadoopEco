/**
 * Copyright (C), 2015-2018, XXX有限公司
 * FileName: ScoreAvg
 * Author:   lvshuo
 * Date:     03/04/2018 2:58 PM
 * Description:
 * History:
 * <author>          <time>          <version>          <desc>
 * 作者姓名           修改时间           版本号              描述
 */
package avg;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
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

import java.io.IOException;
import java.util.Iterator;
import java.util.StringTokenizer;

/**
 * 〈一句话功能简述〉<br> 
 * 〈〉
 *
 * @author lvshuo
 * @create 03/04/2018
 * @since 1.0.0
 */
public class ScoreAvg {

    public static class AvgMapper extends Mapper<LongWritable, Text, Text, DoubleWritable>{

        @Override
        public void map(LongWritable key, Text value, Context context)
                throws IOException, InterruptedException {
            String line = value.toString();
            StringTokenizer itr = new StringTokenizer(line, "\n");
            while (itr.hasMoreElements()){
                StringTokenizer tokenizerLine = new StringTokenizer(itr.nextToken());
                String strName = tokenizerLine.nextToken();
                String strScore = tokenizerLine.nextToken();
                Text name = new Text(strName);
                double score = Double.parseDouble(strScore);
                context.write(name, new DoubleWritable(score));
            }
        }
    }

    public static class AvgReducer extends Reducer<Text, DoubleWritable, Text, DoubleWritable>{

        @Override
        protected void reduce(Text key, Iterable<DoubleWritable> values, Context context)
                throws IOException, InterruptedException {
            int sum = 0;
            int count = 0;

            Iterator<DoubleWritable> iterator = values.iterator();
            while (iterator.hasNext()){
                sum += iterator.next().get();
                count ++;
            }
            double avg_scrore = (double) sum / count;
            context.write(key, new DoubleWritable(avg_scrore));
        }
    }

    public static void main(String[] args) throws Exception{

        if (args.length != 2){
            System.err.println("Input: Output <input path> <output path>");
            System.exit(-1);
        }

        FileUtil.deleteDir("output");
        Configuration conf = new Configuration();

        Job job = new Job(conf, "find average score");
        job.setJarByClass(ScoreAvg.class);
        job.setMapperClass(ScoreAvg.AvgMapper.class);
        job.setReducerClass(ScoreAvg.AvgReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(DoubleWritable.class);

        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);

        FileInputFormat.setInputPaths(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        boolean success = job.waitForCompletion(true);
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }

}