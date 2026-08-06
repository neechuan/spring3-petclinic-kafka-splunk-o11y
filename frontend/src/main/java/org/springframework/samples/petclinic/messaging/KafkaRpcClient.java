/*
 * Copyright 2012-2025 the original author or authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License").
 */
package org.springframework.samples.petclinic.messaging;

import java.time.Duration;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import com.fasterxml.jackson.databind.JavaType;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.type.TypeFactory;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.dao.DataIntegrityViolationException;
import org.springframework.kafka.requestreply.ReplyingKafkaTemplate;
import org.springframework.kafka.requestreply.RequestReplyFuture;
import org.springframework.stereotype.Component;

/**
 * Synchronous request/reply client for the backend over Kafka. Drop-in replacement for
 * the former {@code SolaceRpcClient}: same public API, same exception mapping.
 */
@Component
public class KafkaRpcClient {

	private static final Logger log = LoggerFactory.getLogger(KafkaRpcClient.class);

	private final ReplyingKafkaTemplate<String, String, String> replyingTemplate;

	private final ObjectMapper json;

	@Value("${kafka.request.timeout-ms:10000}")
	private long timeoutMs;

	public KafkaRpcClient(ReplyingKafkaTemplate<String, String, String> replyingTemplate, ObjectMapper json) {
		this.replyingTemplate = replyingTemplate;
		this.json = json;
	}

	public TypeFactory getTypeFactory() {
		return json.getTypeFactory();
	}

	public <T> T call(String operation, Object payload, Class<T> type) {
		return convert(callRaw(operation, payload), getTypeFactory().constructType(type));
	}

	public <T> T call(String operation, Object payload, JavaType type) {
		return convert(callRaw(operation, payload), type);
	}

	private <T> T convert(JsonNode node, JavaType type) {
		if (node == null || node.isNull()) {
			return null;
		}
		return json.convertValue(node, type);
	}

	public JsonNode callRaw(String operation, Object payload) {
		String topic = RpcTopics.PREFIX + operation;
		try {
			String body = (payload == null) ? "{}" : json.writeValueAsString(payload);
			ProducerRecord<String, String> record = new ProducerRecord<>(topic, body);
			RequestReplyFuture<String, String, String> future =
					replyingTemplate.sendAndReceive(record, Duration.ofMillis(timeoutMs));
			ConsumerRecord<String, String> reply = future.get(timeoutMs, TimeUnit.MILLISECONDS);
			RpcResponse response = json.readValue(reply.value(), RpcResponse.class);
			if (!response.isSuccess()) {
				throw toException(response);
			}
			return response.getPayload();
		}
		catch (ExecutionException ex) {
			throw new IllegalStateException("Kafka RPC call failed for operation '" + operation + "'", ex.getCause());
		}
		catch (InterruptedException ex) {
			Thread.currentThread().interrupt();
			throw new IllegalStateException("Kafka RPC call interrupted for operation '" + operation + "'", ex);
		}
		catch (TimeoutException ex) {
			throw new IllegalStateException("Kafka RPC call timed out for operation '" + operation + "'", ex);
		}
		catch (com.fasterxml.jackson.core.JacksonException ex) {
			throw new IllegalStateException("Failed to (de)serialize RPC message for operation '" + operation + "'", ex);
		}
	}

	private RuntimeException toException(RpcResponse response) {
		String code = response.getErrorCode();
		String message = response.getErrorMessage();
		if ("DUPLICATE_PET_NAME".equals(code)) {
			return new DataIntegrityViolationException("unique_owner_pet_name violation: " + message);
		}
		return new IllegalStateException("Backend RPC error [" + code + "]: " + message);
	}

}
