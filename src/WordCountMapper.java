/**
 * Copyright (C), 2015-2018, XXX有限公司
 * FileName: WordCountMapper
 * Author:   lvshuo
 * Date:     27/03/2018 2:52 PM
 * Description: Map part implement
 * History:
 * <author>          <time>          <version>          <desc>
 * 作者姓名           修改时间           版本号              描述
 */

/**
 * 〈一句话功能简述〉<br> 
 * 〈Map part implement〉
 *
 * @author lvshuo
 * @create 27/03/2018
 * @since 1.0.0
 */
import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class WordCountMapper
        extends Mapper<LongWritable, Text, Text, IntWritable> {

    public static final IntWritable one = new IntWritable(1);
    private Text word = new Text();

    @Override
    public void map(LongWritable key, Text value, Context context)
            throws IOException, InterruptedException{

        String line = value.toString();
        StringTokenizer itr = new StringTokenizer(line);
        while(itr.hasMoreTokens()){
            word.set(itr.nextToken());
            context.write(word, one);
        }
    }

}