package com.gauravg.core.main;


import java.lang.reflect.Type;
import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.lang.reflect.ParameterizedType; 


import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.header.internals.RecordHeader;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.kafka.config.ConcurrentKafkaListenerContainerFactory;
import org.springframework.kafka.config.KafkaListenerContainerFactory;
import org.springframework.kafka.core.ConsumerFactory;
import org.springframework.kafka.core.DefaultKafkaConsumerFactory;
import org.springframework.kafka.core.DefaultKafkaProducerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.core.ProducerFactory;
import org.springframework.kafka.listener.ConcurrentMessageListenerContainer;
import org.springframework.kafka.listener.KafkaMessageListenerContainer;
import org.springframework.kafka.listener.config.ContainerProperties;
import org.springframework.kafka.requestreply.ReplyingKafkaTemplate;
import org.springframework.kafka.requestreply.RequestReplyFuture;
import org.springframework.kafka.support.KafkaHeaders;
import org.springframework.kafka.support.SendResult;
import org.springframework.kafka.support.serializer.JsonDeserializer;
import org.springframework.kafka.support.serializer.JsonSerializer;
import org.springframework.stereotype.Component;

import com.gauravg.model.Model;


@Component
public class SyncToAsyncTemplate<V, R, S, M> {

    private final byte[] serviceName=null;
    private final byte[] operationName=null;

	  //@Value("${kafka.bootstrap-servers}")
	  private String bootstrapServers="localhost:9092";
	  
	  @Value("${customer.service.kafka.topic.requestreply-topic}")
	  private String requestReplyTopic;
	  
	  @Value("${customer.kafka.consumergroup}")
	  private String consumerGroup;
	
	  @Value("${customer.service.kafka.topic.request-topic}")
		String customerRequestTopic;
		
		@Value("${customer.service.kafka.topic.requestreply-topic}")
		String customerRequestReplyTopic;
		
		ReplyingKafkaTemplate<String, Model,Model> customerKafkaTemplate;


//    public SyncToAsyncTemplate(String serviceName, String operationName, ReplyingKafkaTemplate<String, Model, Model> rkt){
//
//    this.serviceName=serviceName.getBytes(StandardCharsets.UTF_8);
//    this.operationName=operationName.getBytes(StandardCharsets.UTF_8);
//
//
//    //this.customerKafkaTemplate=buildReplyingKafkaTemplate();
//    
//    this.customerKafkaTemplate=rkt;
//    
//    
//
//    }
    
		
	@SuppressWarnings("unchecked") 		
    public SyncToAsyncTemplate(ReplyingKafkaTemplate<String, Model, Model> replyingKafkaTemplate){

//		Class<S> serviceClass=(Class<S>) ((ParameterizedType) getClass() 
//                .getGenericSuperclass()).getActualTypeArguments()[2];
//		
//		Class<M> methodClass=(Class<M>) ((ParameterizedType) getClass() 
//                .getGenericSuperclass()).getActualTypeArguments()[3];
//		
//		this.serviceName=serviceClass
//				.getCanonicalName()
//    			.getBytes(StandardCharsets.UTF_8);
//		
//		this.operationName=methodClass
//				.getCanonicalName()
//    			.getBytes(StandardCharsets.UTF_8);
		
//    	this.serviceName = ((Class<S>) ((ParameterizedType) getClass()
//                                .getGenericSuperclass()).getActualTypeArguments()[2])
//				    			.getCanonicalName()
//				    			.getBytes(StandardCharsets.UTF_8);
//    	
//        this.operationName=((Class<M>) ((ParameterizedType) getClass()
//				                .getGenericSuperclass()).getActualTypeArguments()[3])
//								.getCanonicalName()
//								.getBytes(StandardCharsets.UTF_8);


        //this.customerKafkaTemplate=buildReplyingKafkaTemplate();
        
        this.customerKafkaTemplate=replyingKafkaTemplate;
        
        

        }

    public Future<R> sendAndRecieve(V v){

        return null;
    }

//    public Future<Model> invoke(Model m){
//    	
//    	return 
//    }
    
    public Model invoke(Model request) throws InterruptedException, ExecutionException {
    	
    	
    	// create producer record
		ProducerRecord<String, Model> record = new ProducerRecord<String, Model>("customer-request-topic", request);
		// set reply topic in header
		record.headers().add(new RecordHeader(KafkaHeaders.REPLY_TOPIC, "customer-requestreply-topic".getBytes()));
		// post in kafka topic
		RequestReplyFuture<String, Model, Model> sendAndReceive = customerKafkaTemplate.sendAndReceive(record);

		// confirm if producer produced successfully
		SendResult<String, Model> sendResult = sendAndReceive.getSendFuture().get();
		
		//print all headers
		sendResult.getProducerRecord().headers().forEach(header -> System.out.println(header.key() + ":" + header.value().toString()));
		
		// get consumer record
		ConsumerRecord<String, Model> consumerRecord = sendAndReceive.get();
		// return consumer value
		return consumerRecord.value();	
    	
    	
    	
    	
    }

    public void setTimeOut(long timeout){


    }
    
    
    
    
    

	  
	  private Map<String, Object> producerConfigs() {
	    Map<String, Object> props = new HashMap<>();
	    // list of host:port pairs used for establishing the initial connections to the Kakfa cluster
	    props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG,
	        bootstrapServers);
	    props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG,
	        StringSerializer.class);
	    props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, JsonSerializer.class);
	    return props;
	  }
	  
	  
	  private Map<String, Object> consumerConfigs() {
	    Map<String, Object> props = new HashMap<>();
	    props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG,bootstrapServers);
	    props.put(ConsumerConfig.GROUP_ID_CONFIG, "helloworld");
	    return props;
	  }

	  
	  private ProducerFactory<String,Model> producerFactory() {
	    return new DefaultKafkaProducerFactory<>(producerConfigs());
	  }
	  
	  
	  private KafkaTemplate<String, Model> kafkaTemplate() {
	    return new KafkaTemplate<>(producerFactory());
	  }
	  
	  
	  private ReplyingKafkaTemplate<String, Model, Model> replyKafkaTemplate(ProducerFactory<String, Model> pf, KafkaMessageListenerContainer<String, Model> container){
		  return new ReplyingKafkaTemplate<>(pf, container);
		  
	  }
	  
	  
	  private KafkaMessageListenerContainer<String, Model> replyContainer(ConsumerFactory<String, Model> cf) {
	        ContainerProperties containerProperties = new ContainerProperties(requestReplyTopic);
	        return new KafkaMessageListenerContainer<>(cf, containerProperties);
	    }
	  
	  
	  private ConsumerFactory<String, Model> consumerFactory() {
	    return new DefaultKafkaConsumerFactory<>(consumerConfigs(),new StringDeserializer(),new JsonDeserializer<>(Model.class));
	  }
	  
	  
	  private KafkaListenerContainerFactory<ConcurrentMessageListenerContainer<String, Model>> kafkaListenerContainerFactory() {
	    ConcurrentKafkaListenerContainerFactory<String, Model> factory = new ConcurrentKafkaListenerContainerFactory<>();
	    factory.setConsumerFactory(consumerFactory());
	    factory.setReplyTemplate(kafkaTemplate());
	    return factory;
	  }
	  
	  private ReplyingKafkaTemplate<String, Model, Model> buildReplyingKafkaTemplate()
	  {
		  
		 return this.replyKafkaTemplate(
				 this.producerFactory(),
				 this.replyContainer(consumerFactory()));
		  
	  }


}


