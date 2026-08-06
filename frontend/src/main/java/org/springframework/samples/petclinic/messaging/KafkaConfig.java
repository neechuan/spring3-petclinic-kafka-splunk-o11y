/*
 * Copyright 2012-2025 the original author or authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License").
 */
package org.springframework.samples.petclinic.messaging;

import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.config.ConcurrentKafkaListenerContainerFactory;
import org.springframework.kafka.core.ProducerFactory;
import org.springframework.kafka.core.ConsumerFactory;
import org.springframework.kafka.listener.ConcurrentMessageListenerContainer;
import org.springframework.kafka.requestreply.ReplyingKafkaTemplate;

@Configuration
public class KafkaConfig {

	/** Reply container: listens on the replies topic; managed by ReplyingKafkaTemplate. */
	@Bean
	public ConcurrentMessageListenerContainer<String, String> repliesContainer(
			ConcurrentKafkaListenerContainerFactory<String, String> factory) {
		ConcurrentMessageListenerContainer<String, String> container =
				factory.createContainer(RpcTopics.REPLIES_TOPIC);
		container.getContainerProperties().setGroupId("petclinic-frontend-replies");
		container.setAutoStartup(false);
		return container;
	}

	@Bean
	public ReplyingKafkaTemplate<String, String, String> replyingKafkaTemplate(
			ProducerFactory<String, String> pf,
			ConcurrentMessageListenerContainer<String, String> repliesContainer) {
		return new ReplyingKafkaTemplate<>(pf, repliesContainer);
	}

}
