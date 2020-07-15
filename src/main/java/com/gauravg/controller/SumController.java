package com.gauravg.controller;

import java.util.concurrent.ExecutionException;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.header.internals.RecordHeader;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.http.MediaType;
import org.springframework.kafka.requestreply.ReplyingKafkaTemplate;
import org.springframework.kafka.requestreply.RequestReplyFuture;
import org.springframework.kafka.support.KafkaHeaders;
import org.springframework.kafka.support.SendResult;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.ResponseBody;
import org.springframework.web.bind.annotation.RestController;

import com.gauravg.core.main.SyncToAsyncTemplate;
import com.gauravg.model.Model;
import com.gauravg.service.CustomerService;
import com.gauravg.service.EmployeeService;

@RestController
public class SumController {
	
	@Autowired
	ReplyingKafkaTemplate<String, Model,Model> kafkaTemplate;
	
	
	@Autowired
	ReplyingKafkaTemplate<String, Model,Model> customerKafkaTemplate;
	
	@Autowired
	SyncToAsyncTemplate<String,String,CustomerService,CustomerService.GetAll> syncToAsyncTemplate;
	
	@Autowired
	SyncToAsyncTemplate<String,String,EmployeeService,EmployeeService.UpdateDataEvent> syncToAsyncTemplate2;
	
	@Value("${kafka.topic.request-topic}")
	String requestTopic;
	
	@Value("${kafka.topic.requestreply-topic}")
	String requestReplyTopic;
	
	
	@Value("${customer.service.kafka.topic.request-topic}")
	String customerRequestTopic;
	
	@Value("${customer.service.kafka.topic.requestreply-topic}")
	String customerRequestReplyTopic;
	
	
	
	@ResponseBody
	@PostMapping(value="/sum",produces=MediaType.APPLICATION_JSON_VALUE,consumes=MediaType.APPLICATION_JSON_VALUE)
	public Model sum(@RequestBody Model request) throws InterruptedException, ExecutionException {
		// create producer record
		ProducerRecord<String, Model> record = new ProducerRecord<String, Model>(requestTopic, request);
		// set reply topic in header
		record.headers().add(new RecordHeader(KafkaHeaders.REPLY_TOPIC, requestReplyTopic.getBytes()));
		// post in kafka topic
		RequestReplyFuture<String, Model, Model> sendAndReceive = kafkaTemplate.sendAndReceive(record);

		// confirm if producer produced successfully
		SendResult<String, Model> sendResult = sendAndReceive.getSendFuture().get();
		
		//print all headers
		sendResult.getProducerRecord().headers().forEach(header -> System.out.println(header.key() + ":" + header.value().toString()));
		
		// get consumer record
		ConsumerRecord<String, Model> consumerRecord = sendAndReceive.get();
		// return consumer value
		return consumerRecord.value();		
	}
	
	
	
	

	@ResponseBody
	@PostMapping(value="/customer",produces=MediaType.APPLICATION_JSON_VALUE,consumes=MediaType.APPLICATION_JSON_VALUE)
	public Model processCustomer(@RequestBody Model request) throws InterruptedException, ExecutionException {
//				// create producer record
//				ProducerRecord<String, Model> record = new ProducerRecord<String, Model>(customerRequestTopic, request);
//				// set reply topic in header
//				record.headers().add(new RecordHeader(KafkaHeaders.REPLY_TOPIC, customerRequestReplyTopic.getBytes()));
//				// post in kafka topic
//				RequestReplyFuture<String, Model, Model> sendAndReceive = customerKafkaTemplate.sendAndReceive(record);
//
//				// confirm if producer produced successfully
//				SendResult<String, Model> sendResult = sendAndReceive.getSendFuture().get();
//				
//				//print all headers
//				sendResult.getProducerRecord().headers().forEach(header -> System.out.println(header.key() + ":" + header.value().toString()));
//				
//				// get consumer record
//				ConsumerRecord<String, Model> consumerRecord = sendAndReceive.get();
//				// return consumer value
//				return consumerRecord.value();	
		
//		SyncToAsyncTemplate<Model, Model> _SyncToAsyncTemplate=new SyncToAsyncTemplate<Model,Model>("customerService","getAll");
//		return _SyncToAsyncTemplate.invoke(request);
		
		return syncToAsyncTemplate.invoke(request);
		
	}
	
	
	@ResponseBody
	@PostMapping(value="/employee",produces=MediaType.APPLICATION_JSON_VALUE,consumes=MediaType.APPLICATION_JSON_VALUE)
	public Model processEmployee(@RequestBody Model request) throws InterruptedException, ExecutionException {
		
		return syncToAsyncTemplate2.invoke(request);
		
	}

}
