/**
 * Copyright (C), 2015-2018, XXX有限公司
 * FileName: RemoveDup
 * Author:   lvshuo
 * Date:     03/04/2018 1:54 PM
 * Description: select distinct(x) from table
 * History:
 * <author>          <time>          <version>          <desc>
 * 作者姓名           修改时间           版本号              描述
 */
package remove_duplicate;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import word_count.FileUtil;

/**
 * 〈一句话功能简述〉<br> 
 * 〈select distinct(x) from table〉
 * 原理:map阶段完成后,在reduce开始之前,会有一个combine的过程,相同的key值会自动合并,所以自然而然的就去掉了重复
 *
 * @author lvshuo
 * @create 03/04/2018
 * @since 1.0.0
 */
public class RemoveDup {

    public static class RemoveDupMapper extends Mapper<Object, Text, Text, NullWritable>{

        @Override
        public void map(Object key, Text value, Context context)
                throws IOException, InterruptedException {
            System.out.println("map: key=" + key + ",value=" + value);
            context.write(value, NullWritable.get());
        }
    }

    public static class RemoveDupReducer extends Reducer<Text, NullWritable, Text, NullWritable>{

        @Override
        public void reduce(Text key, Iterable<NullWritable> values, Context context) throws IOException, InterruptedException {

            System.out.println("reduce: key=" + key);
            context.write(key, NullWritable.get());
        }
    }

    public static void main(String[] args) throws Exception{

        if (args.length != 2){
            System.err.println("Input: Output <input path> <output path>");
            System.exit(-1);
        }

        FileUtil.deleteDir("output");
        Configuration conf = new Configuration();

        Job job = new Job(conf, "remove duplicates");
        job.setJarByClass(RemoveDup.class);
        job.setMapperClass(RemoveDup.RemoveDupMapper.class);
        job.setCombinerClass(RemoveDup.RemoveDupReducer.class);
        job.setReducerClass(RemoveDup.RemoveDupReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(NullWritable.class);

        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);

        FileInputFormat.setInputPaths(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        boolean success = job.waitForCompletion(true);
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }

}