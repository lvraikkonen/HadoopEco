/**
 * Copyright (C), 2015-2018, XXX有限公司
 * FileName: WordCountReducer
 * Author:   lvshuo
 * Date:     27/03/2018 3:02 PM
 * Description: Reduce part implement
 * History:
 * <author>          <time>          <version>          <desc>
 * 作者姓名           修改时间           版本号              描述
 */

/**
 * 〈一句话功能简述〉<br> 
 * 〈Reduce part implement〉
 *
 * @author lvshuo
 * @create 27/03/2018
 * @since 1.0.0
 */
import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class WordCountReducer extends Reducer<Text, IntWritable, Text, IntWritable> {

    @Override
    public void reduce(Text key, Iterable<IntWritable> values, Context context)
            throws IOException, InterruptedException {

        int sum = 0;
        for(IntWritable val: values){
            sum += val.get();
        }
        context.write(key, new IntWritable(sum));
    }
}