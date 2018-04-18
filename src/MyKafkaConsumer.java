/**
 * Copyright (C), 2015-2018, XXX有限公司
 * FileName: MyKafkaConsumer
 * Author:   lvshuo
 * Date:     2018/4/18 3:14 PM
 * Description:
 * History:
 * <author>          <time>          <version>          <desc>
 * 作者姓名           修改时间           版本号              描述
 */

import java.util.Collections;
import java.util.Properties;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.ConsumerRecords;

/**
 * 〈一句话功能简述〉<br> 
 * 〈kafka producer to read message to kafka broker〉
 *
 * @author lvshuo
 * @create 2018/4/18
 * @since 1.0.0
 */
public class MyKafkaConsumer {


    public static void main(String[] args) {

        String topicName = args[0].toString();
        // set properties
        Properties props = new Properties();
        props.put("metadata.broker.list", "127.0.0.1:2181");
        props.put("bootstrap.servers", "127.0.0.1:9092");
        props.put("group.id", "javaConsumer");
        props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        props.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        //create comsumer
        KafkaConsumer<String, String> consumer = new KafkaConsumer<String, String>(props);
        // Subscribe to the topic
        consumer.subscribe(Collections.singletonList("myfirsttopic"));

        // query
        try {
            while (true){
                ConsumerRecords<String, String> records = consumer.poll(100);
                for (ConsumerRecord<String, String> record : records){
                    System.out.printf("Consumer Record:(%s, %s, %s, %s, %s)\n",
                            record.key(), record.value(),
                            record.partition(), record.offset(), record.topic());

                }
            }
        } finally {
            consumer.close();
        }
    }

}