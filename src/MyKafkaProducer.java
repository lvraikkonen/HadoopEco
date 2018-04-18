/**
 * Copyright (C), 2015-2018, XXX有限公司
 * FileName: MyKafkaProducer
 * Author:   lvshuo
 * Date:     2018/4/18 11:40 AM
 * Description: kafka producer to send message to kafka broker
 * History:
 * <author>          <time>          <version>          <desc>
 * 作者姓名           修改时间           版本号              描述
 */

import java.util.Properties;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;

/**
 * 〈一句话功能简述〉<br> 
 * 〈kafka producer to send message to kafka broker〉
 *
 * @author lvshuo
 * @create 2018/4/18
 * @since 1.0.0
 */
public class MyKafkaProducer {

    public static void main(String[] args) throws Exception {
        if (args.length == 0) {
            System.out.println("Enter topic name");
            return;
        }

        String topicName = args[0].toString();
        // set properties
        Properties props = new Properties();
        props.put("metadata.broker.list", "127.0.0.1:2181");
        props.put("bootstrap.servers", "127.0.0.1:9092");
        props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        props.put("request.required.acks", "1");

        // create producer
        Producer<String, String> producer = new KafkaProducer<String, String>(props);

        for (int i = 0; i < 10; i++){
            producer.send(new ProducerRecord<String, String>(topicName, Integer.toString(i), Integer.toString(i)));
            System.out.println("Message sent successfully");
        }
        producer.close();
    }

}