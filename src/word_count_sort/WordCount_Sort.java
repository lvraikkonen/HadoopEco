/**
 * Copyright (C), 2015-2018, XXX有限公司
 * FileName: WordCount_Sort
 * Author:   lvshuo
 * Date:     03/04/2018 11:59 AM
 * Description: 统计出单词出现的次数,并按词频做倒排
 * History:
 * <author>          <time>          <version>          <desc>
 * 作者姓名           修改时间           版本号              描述
 */
package word_count_sort;

import java.io.IOException;
import java.util.Comparator;
import java.util.StringTokenizer;
import java.util.TreeMap;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
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
 * 〈统计出单词出现的次数,并按词频做倒排〉
 *
 * @author lvshuo
 * @create 03/04/2018
 * @since 1.0.0
 */
public class WordCount_Sort {

    public static class TokenizerMapper extends Mapper<Object, Text, Text, IntWritable>{

        private static final IntWritable one = new IntWritable(1);
        private Text word = new Text();

        @Override
        protected void map(Object key, Text value, Context context)
                throws IOException, InterruptedException {
            StringTokenizer itr = new StringTokenizer(value.toString());
            while (itr.hasMoreTokens()){
                word.set(itr.nextToken());
                context.write(word, one);
            }
        }
    }

    public static class IntReducer extends Reducer<Text, IntWritable, Text, IntWritable>{

        // treeMap 保持结果有序
        private TreeMap<Integer, String> treeMap = new TreeMap<Integer, String>(new Comparator<Integer>() {
            @Override
            public int compare(Integer o1, Integer o2) {
                return o2.compareTo(o1);
            }
        });

        @Override
        protected void reduce(Text key, Iterable<IntWritable> values, Context context)
                throws IOException, InterruptedException {
            //reduce 后的结果放入treeMap
            int sum = 0;
            for (IntWritable val: values){
                sum += val.get();
            }
            if (treeMap.containsKey(sum)){
                String value = treeMap.get(sum) + ", " + key.toString();
                treeMap.put(sum, value);
            }
            else
                treeMap.put(sum, key.toString());
        }

        @Override
        protected void cleanup(Context context)
                throws IOException, InterruptedException{
            // treeMap 中的结果，按value-key顺序写入context
            for (Integer key: treeMap.keySet()){
                context.write(new Text(treeMap.get(key)), new IntWritable(key));
            }
        }
    }

    public static void main(String[] args) throws Exception {
        if (args.length != 2){
            System.err.println("Input: Output <input path> <output path>");
            System.exit(-1);
        }

        FileUtil.deleteDir("output");
        Configuration conf = new Configuration();

        Job job = new Job(conf, "word count 2");
        job.setJarByClass(WordCount_Sort.class);
        job.setMapperClass(TokenizerMapper.class);
        job.setCombinerClass(IntReducer.class);
        job.setReducerClass(IntReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);

        FileInputFormat.setInputPaths(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        boolean success = job.waitForCompletion(true);
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }

}