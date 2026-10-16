/*
 * Copyright 2025 Google LLC
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
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
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;

public class CloudRunClientWrapper {
  private static final FluentLogger logger = FluentLogger.forEnclosingClass();
  private static final MetadataKey<String> RESOURCE = MetadataKey.single("resource", String.class);

  private final String pubSubTopic;
  // Caché en memoria para almacenar la última cantidad de instancias enviada por cada recurso
  private final Map<String, Integer> lastSentInstancesCache = new ConcurrentHashMap<>();

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
   * Publica la orden de escalado únicamente si la cantidad de instancias requeridas cambió respecto al ciclo anterior.
   * Permite explícitamente el envío de '0' para asegurar el desescalado en Cloud Run.
   */
  public void updateServiceMinInstances(
      String serviceName, int instances, String projectId, String region)
      throws ExecutionException, InterruptedException, IOException {

    String resourceName = String.format("projects/%s/locations/%s/services/%s", projectId, region, serviceName);

    // 1. Verificar el último valor enviado en el ciclo anterior
    Integer lastValue = lastSentInstancesCache.get(resourceName);
    
    // Si el valor recomendado (ej. 0 o 1) es IDÉNTICO al último enviado con éxito a Pub/Sub, se omite.
    if (lastValue != null && lastValue == instances) {
      logger.atInfo()
          .with(RESOURCE, resourceName)
          .log("[SCALER] Sin cambios para %s (instancias actuales en caché: %d). Se omite envío.", resourceName, instances);
      return;
    }

    TopicName topicName = TopicName.of(projectId, this.pubSubTopic);
    Publisher publisher = null;

    try {
      publisher = Publisher.newBuilder(topicName).build();

      // Payload JSON estandarizado para la Cloud Function
      String jsonPayload = String.format(
          "{\"service\":\"%s\", \"project\":\"%s\", \"region\":\"%s\", \"min\":%d, \"min_instances\":%d}",
          serviceName, projectId, region, instances, instances
      );

      PubsubMessage pubsubMessage = PubsubMessage.newBuilder()
          .setData(ByteString.copyFromUtf8(jsonPayload))
          .build();

      // Publica y espera la confirmación (sincrónico)
      publisher.publish(pubsubMessage).get();

      // 2. SOLO si la publicación en Pub/Sub fue exitosa, actualizamos la caché
      lastSentInstancesCache.put(resourceName, instances);

      logger.atInfo()
          .with(RESOURCE, resourceName)
          .log(" Transición de escalado exitosa para %s: [%s -> %d]. Orden enviada a Pub/Sub (%s)", 
               resourceName, (lastValue == null ? "N/A" : lastValue), instances, this.pubSubTopic);

    } catch (Exception e) {
      logger.atSevere()
          .with(RESOURCE, resourceName)
          .log("Error al publicar la orden de escalado (--min=%d) a Pub/Sub: %s", instances, e.getMessage());
      throw e;
    } finally {
      if (publisher != null) {
        try {
          publisher.shutdown();
          publisher.awaitTermination(5, TimeUnit.SECONDS);
        } catch (Exception e) {
          // Ignorar excepciones al cerrar recursos del cliente
        }
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
