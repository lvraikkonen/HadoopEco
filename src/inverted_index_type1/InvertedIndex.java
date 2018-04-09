/**
 * Copyright (C), 2015-2018, XXX有限公司
 * FileName: InvertedIndex
 * Author:   lvshuo
 * Date:     09/04/2018 3:40 PM
 * Description: 倒排索引：第一类
 * History:
 * <author>          <time>          <version>          <desc>
 * 作者姓名           修改时间           版本号              描述
 */
package inverted_index_type1;

import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

/**
 * 〈一句话功能简述〉<br> 
 * 〈倒排索引：第一类〉
 *
 * @author lvshuo
 * @create 09/04/2018
 * @since 1.0.0
 */
public class InvertedIndex {

    // 在Map端，把单词和文件名作为key值，把单词词频作为value值
    public static class InvertedIndexMapper extends Mapper<Object, Text, Text, Text>{

        private Text word_filename_key = new Text();
        private Text word_frequency = new Text();
        private FileSplit split; // split对象

        @Override
        public void map(Object key, Text value, Context context)
                throws IOException, InterruptedException {
            //获得<key,value>对所属的FileSplit对象
            split = (FileSplit) context.getInputSplit();

            StringTokenizer itr = new StringTokenizer(value.toString());
            while (itr.hasMoreTokens()){
                word_filename_key.set(itr.nextToken() + ":" + split.getPath().toString());
                word_frequency.set("1");
                context.write(word_filename_key, word_frequency);
            }
        }
        //map output:   I:1.txt    list{1,1,1}
    }

    // Combiner merge word frequency
    public static class InvertedIndexCombiner extends Reducer<Text, Text, Text, Text>{

        private Text new_value = new Text();

        @Override
        public void reduce(Text key, Iterable<Text> values, Context context)
                throws IOException, InterruptedException {
            int sum = 0;
            for (Text value: values){
                sum += Integer.parseInt(value.toString());
            }
            int splitIndex = key.toString().indexOf(":");

            // 文件名和词频作为value值
            new_value.set(key.toString().substring(splitIndex + 1) + ":" + sum);

            // 单词设成key
            key.set(key.toString().substring(0, splitIndex));

            context.write(key, new_value);

            System.out.println("key: " + key);
            System.out.println("value: " + new_value);
        }
        // combiner output: I         1.txt:3
    }

    // 生成单词的文档列表
    public static class InvertedIndexReducer extends Reducer<Text, Text, Text, Text>{

        private Text result = new Text();

        @Override
        public void reduce(Text key, Iterable<Text> values, Context context)
                throws IOException, InterruptedException {
            String fileList = "";
            for (Text value: values){
                fileList += " " + value.toString() + ";";
            }
            result.set(fileList);
            context.write(key, result);
        }
    }


    public static void main(String[] args) throws Exception{
        if (args.length != 2){
            System.err.println("Input: Output <input path> <output path>");
            System.exit(-1);
        }

        FileUtil.deleteDir("output");
        Configuration conf = new Configuration();

        Job job = new Job(conf, "my inverted index");
        job.setJarByClass(InvertedIndex.class);

        job.setMapperClass(InvertedIndex.InvertedIndexMapper.class);
        job.setCombinerClass(InvertedIndex.InvertedIndexCombiner.class);
        job.setReducerClass(InvertedIndex.InvertedIndexReducer.class);

        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        FileInputFormat.setInputPaths(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        boolean success = job.waitForCompletion(true);
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }

}