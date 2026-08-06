/*
 * Copyright 2012-2025 the original author or authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License").
 */
package org.springframework.samples.petclinic.messaging;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.support.KafkaHeaders;
import org.springframework.messaging.handler.annotation.Header;
import org.springframework.messaging.handler.annotation.SendTo;
import org.springframework.samples.petclinic.messaging.dto.RpcResponse;
import org.springframework.stereotype.Component;

/**
 * Kafka replier. Listens on all six RPC request topics, dispatches each message to
 * {@link PetClinicRpcService}, and returns the JSON reply via {@code @SendTo} which
 * routes it to the {@code KafkaHeaders.REPLY_TOPIC} header set by the frontend's
 * {@code ReplyingKafkaTemplate}.
 */
@Component
public class KafkaRpcListener {

	private static final Logger log = LoggerFactory.getLogger(KafkaRpcListener.class);

	private final PetClinicRpcService service;

	private final ObjectMapper json;

	public KafkaRpcListener(PetClinicRpcService service, ObjectMapper json) {
		this.service = service;
		this.json = json;
	}

	@KafkaListener(topics = {
		RpcTopics.PREFIX + RpcTopics.OWNER_FIND_BY_ID,
		RpcTopics.PREFIX + RpcTopics.OWNER_FIND_BY_LAST_NAME,
		RpcTopics.PREFIX + RpcTopics.OWNER_SAVE,
		RpcTopics.PREFIX + RpcTopics.PETTYPE_FIND_ALL,
		RpcTopics.PREFIX + RpcTopics.VET_FIND_ALL,
		RpcTopics.PREFIX + RpcTopics.VET_FIND_ALL_PAGED
	}, groupId = "${spring.kafka.consumer.group-id}")
	@SendTo
	public String handle(String body, @Header(KafkaHeaders.RECEIVED_TOPIC) String topic) {
		String operation = topic.substring(RpcTopics.PREFIX.length());
		log.debug("RPC request on topic '{}' operation '{}'", topic, operation);
		RpcResponse response = service.dispatch(operation, body);
		try {
			return json.writeValueAsString(response);
		}
		catch (JsonProcessingException ex) {
			log.error("Failed to serialize RPC reply for operation '{}'", operation, ex);
			try {
				return json.writeValueAsString(RpcResponse.error("SERIALIZATION_ERROR", ex.getMessage()));
			}
			catch (JsonProcessingException ignored) {
				return "{\"success\":false,\"errorCode\":\"SERIALIZATION_ERROR\"}";
			}
		}
	}

}
