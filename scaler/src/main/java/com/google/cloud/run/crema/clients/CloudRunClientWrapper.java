/*
 * Copyright 2025 Google LLC
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 */

package com.google.cloud.run.crema.clients;

import com.google.cloud.pubsub.v1.Publisher;
import com.google.common.flogger.FluentLogger;
import com.google.common.flogger.MetadataKey;
import com.google.protobuf.ByteString;
import com.google.pubsub.v1.PubsubMessage;
import com.google.pubsub.v1.TopicName;
import java.io.IOException;
import java.time.Instant;
import java.util.concurrent.ExecutionException;

public class CloudRunClientWrapper {
  private static final FluentLogger logger = FluentLogger.forEnclosingClass();
  private static final MetadataKey<String> RESOURCE = MetadataKey.single("resource", String.class);

  private final String pubSubTopic;

  /**
   * Constructor: Obtiene el tópico de Pub/Sub estrictamente de la variable de entorno PUB_SUB_TOPIC.
   */
  public CloudRunClientWrapper() {
    this.pubSubTopic = System.getenv("PUB_SUB_TOPIC");
    if (this.pubSubTopic == null || this.pubSubTopic.trim().isEmpty()) {
      throw new IllegalStateException(
          "La variable de entorno 'PUB_SUB_TOPIC' no está definida en el contenedor."
      );
    }
  }

  public int getServiceInstanceCount(String projectId, String region, String serviceName) {
    return 0;
  }

  public int getWorkerPoolInstanceCount(String projectId, String region, String workerpoolName) {
    return 0;
  }

  public Instant getServiceLastDeploymentTime(String serviceName, String projectId, String region) {
    return Instant.EPOCH;
  }

  public Instant getWorkerPoolLastDeploymentTime(String workerpoolName, String projectId, String region) {
    return Instant.EPOCH;
  }

  /**
   * Publica el mensaje con el min_instances y la información del servicio hacia Pub/Sub.
   */
  public void updateServiceMinInstances(
      String serviceName, int instances, String projectId, String region)
      throws ExecutionException, InterruptedException, IOException {

    String resourceName = String.format("projects/%s/locations/%s/services/%s", projectId, region, serviceName);
    TopicName topicName = TopicName.of(projectId, this.pubSubTopic);
    Publisher publisher = null;

    try {
      publisher = Publisher.newBuilder(topicName).build();

      String jsonPayload = String.format(
          "{\"service\":\"%s\", \"project\":\"%s\", \"region\":\"%s\", \"min_instances\":\"%d\"}",
          serviceName, projectId, region, instances
      );

      PubsubMessage pubsubMessage = PubsubMessage.newBuilder()
          .setData(ByteString.copyFromUtf8(jsonPayload))
          .build();

      publisher.publish(pubsubMessage).get();

      logger.atInfo()
          .with(RESOURCE, resourceName)
          .log("Órden de escalado enviada a Pub/Sub (%s) para %s -> --min=%d", this.pubSubTopic, resourceName, instances);

    } finally {
      if (publisher != null) {
        publisher.shutdown();
      }
    }
  }

  public void updateServiceManualInstances(
      String serviceName, int instances, String projectId, String region)
      throws ExecutionException, InterruptedException, IOException {
    updateServiceMinInstances(serviceName, instances, projectId, region);
  }

  public void updateWorkerPoolManualInstances(
      String workerpoolName, int instances, String projectId, String region)
      throws ExecutionException, InterruptedException, IOException {
    updateServiceMinInstances(workerpoolName, instances, projectId, region);
  }
}
