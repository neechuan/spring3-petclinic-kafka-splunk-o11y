/*
 * Copyright 2012-2025 the original author or authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License").
 */
package org.springframework.samples.petclinic.messaging;

import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.config.ConcurrentKafkaListenerContainerFactory;
import org.springframework.kafka.config.TopicBuilder;
import org.springframework.kafka.core.ConsumerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.listener.ConcurrentMessageListenerContainer;
import org.apache.kafka.clients.admin.NewTopic;

@Configuration
public class KafkaConfig {

	/** Wire the auto-configured KafkaTemplate as the reply sender for @SendTo. */
	@Bean
	public ConcurrentKafkaListenerContainerFactory<String, String> kafkaListenerContainerFactory(
			ConsumerFactory<String, String> cf, KafkaTemplate<String, String> replyTemplate) {
		ConcurrentKafkaListenerContainerFactory<String, String> factory =
				new ConcurrentKafkaListenerContainerFactory<>();
		factory.setConsumerFactory(cf);
		factory.setReplyTemplate(replyTemplate);
		return factory;
	}

	@Bean
	public NewTopic topicOwnerFindById() {
		return TopicBuilder.name(RpcTopics.PREFIX + RpcTopics.OWNER_FIND_BY_ID).partitions(1).replicas(1).build();
	}

	@Bean
	public NewTopic topicOwnerFindByLastName() {
		return TopicBuilder.name(RpcTopics.PREFIX + RpcTopics.OWNER_FIND_BY_LAST_NAME).partitions(1).replicas(1).build();
	}

	@Bean
	public NewTopic topicOwnerSave() {
		return TopicBuilder.name(RpcTopics.PREFIX + RpcTopics.OWNER_SAVE).partitions(1).replicas(1).build();
	}

	@Bean
	public NewTopic topicPettypeFindAll() {
		return TopicBuilder.name(RpcTopics.PREFIX + RpcTopics.PETTYPE_FIND_ALL).partitions(1).replicas(1).build();
	}

	@Bean
	public NewTopic topicVetFindAll() {
		return TopicBuilder.name(RpcTopics.PREFIX + RpcTopics.VET_FIND_ALL).partitions(1).replicas(1).build();
	}

	@Bean
	public NewTopic topicVetFindAllPaged() {
		return TopicBuilder.name(RpcTopics.PREFIX + RpcTopics.VET_FIND_ALL_PAGED).partitions(1).replicas(1).build();
	}

}
