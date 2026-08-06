/*
 * Copyright 2012-2025 the original author or authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License").
 */
package org.springframework.samples.petclinic.owner;

import java.util.List;

import com.fasterxml.jackson.databind.JavaType;

import org.springframework.samples.petclinic.messaging.KafkaRpcClient;
import org.springframework.samples.petclinic.messaging.RpcTopics;
import org.springframework.stereotype.Repository;

/**
 * Kafka-backed {@link PetTypeRepository}.
 */
@Repository
public class KafkaPetTypeRepository implements PetTypeRepository {

	private final KafkaRpcClient client;

	public KafkaPetTypeRepository(KafkaRpcClient client) {
		this.client = client;
	}

	@Override
	public List<PetType> findPetTypes() {
		JavaType type = client.getTypeFactory().constructCollectionType(List.class, PetType.class);
		List<PetType> types = client.call(RpcTopics.PETTYPE_FIND_ALL, null, type);
		return types != null ? types : List.of();
	}

}
