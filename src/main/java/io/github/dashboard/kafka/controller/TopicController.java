/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package io.github.dashboard.kafka.controller;

import io.github.dashboard.kafka.module.CreateTopicReq;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.CreateTopicsResult;
import org.apache.kafka.clients.admin.ListTopicsResult;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.admin.TopicListing;
import org.apache.kafka.common.KafkaFuture;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PutMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;
import reactor.core.publisher.Mono;

import java.util.Collections;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;

@Slf4j
@RestController
@RequestMapping("/api/kafka")
public class TopicController {

    private final AdminClient adminClient;

    public TopicController(@Autowired AdminClient adminClient) {
        this.adminClient = adminClient;
    }

    @PutMapping("/topics")
    public Mono<ResponseEntity<Void>> createTopic(@RequestBody CreateTopicReq req) {
        CompletableFuture<ResponseEntity<Void>> future = new CompletableFuture<>();
        NewTopic newTopic = new NewTopic(req.getName(), Optional.empty(), Optional.empty());
        CreateTopicsResult createTopicsResult = adminClient.createTopics(Collections.singleton(newTopic));
        createTopicsResult.all().whenComplete((map, throwable) -> {
            if (throwable != null) {
                log.error("create topic {} error", req.getName(), throwable);
                future.complete(ResponseEntity.internalServerError().build());
            } else {
                future.complete(ResponseEntity.ok().build());
            }
        });
        return Mono.fromFuture(future);
    }

    @GetMapping("/topics")
    public Mono<ResponseEntity<Map<String, TopicListing>>> listTopics() {
        CompletableFuture<ResponseEntity<Map<String, TopicListing>>> future = new CompletableFuture<>();
        ListTopicsResult listTopicsResult = adminClient.listTopics();
        KafkaFuture<Map<String, TopicListing>> kafkaFuture = listTopicsResult.namesToListings();
        kafkaFuture.whenComplete((map, throwable) -> {
            if (throwable != null) {
                log.error("Error while listing topics", throwable);
                future.complete(ResponseEntity.internalServerError().build());
            } else {
                future.complete(ResponseEntity.ok(map));
            }
        });
        return Mono.fromFuture(future);
    }
}
