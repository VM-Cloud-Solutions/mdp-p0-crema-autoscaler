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
import com.google.cloud.run.v2.GetServiceRequest;
import com.google.cloud.run.v2.Service;
import com.google.cloud.run.v2.ServiceName;
import com.google.cloud.run.v2.ServicesClient;
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
  private final ServicesClient servicesClient;
  private final Map<String, Integer> lastSentInstancesCache = new ConcurrentHashMap<>();

  /**
   * Constructor: Inicializa el cliente oficial de Cloud Run v2 
   * y obtiene el tópico de Pub/Sub desde la variable de entorno PUB_SUB_TOPIC.
   */
  public CloudRunClientWrapper() throws IOException {
    this.pubSubTopic = System.getenv("PUB_SUB_TOPIC");
    if (this.pubSubTopic == null || this.pubSubTopic.trim().isEmpty()) {
      throw new IllegalStateException(
          "La variable de entorno 'PUB_SUB_TOPIC' no está definida en el contenedor."
      );
    }
    this.servicesClient = ServicesClient.create();
  }

  public CloudRunClientWrapper(ServicesClient servicesClient) {
    this.pubSubTopic = System.getenv("PUB_SUB_TOPIC");
    this.servicesClient = servicesClient;
  }

  /**
   * Consulta en tiempo real el parámetro --min configurado en el servicio.
   * Maneja tanto Auto scaling como Manual scaling en la API v2 de Cloud Run.
   */
  public int getServiceMinParam(String serviceName, String projectId, String region) {
    String resourceName = String.format("projects/%s/locations/%s/services/%s", projectId, region, serviceName);
    int currentMin = 0;

    try {
      ServiceName name = ServiceName.of(projectId, region, serviceName);
      GetServiceRequest getRequest = GetServiceRequest.newBuilder().setName(name.toString()).build();
      Service service = servicesClient.getService(getRequest);

      // 1. Revisar si la plantilla (template) tiene el escalado configurado
      if (service.hasTemplate() && service.getTemplate().hasScaling()) {
        currentMin = service.getTemplate().getScaling().getMinInstanceCount();
      }

      // 2. Si a nivel de plantilla es 0, verificar si está configurado a nivel de servicio (Service scaling)
      if (currentMin == 0 && service.hasScaling()) {
        // Si el modo es MANUAL, lee manualInstanceCount
        if (service.getScaling().getScalingMode() == com.google.cloud.run.v2.ServiceScaling.ScalingMode.MANUAL) {
          currentMin = service.getScaling().getManualInstanceCount();
        } 
        // Si está en Auto scaling, lee minInstanceCount a nivel de plantilla de servicio
        else if (service.getScaling().getMinInstanceCount() > 0) {
          currentMin = service.getScaling().getMinInstanceCount();
        }
      }

      // Registro explícito en el LOG
      logger.atInfo()
          .with(RESOURCE, resourceName)
          .log("status actual de instancias del servicio (parametro --min) = %d", currentMin);

    } catch (Exception e) {
      logger.atWarning()
          .with(RESOURCE, resourceName)
          .log("No se pudo obtener el parámetro --min para %s: %s", serviceName, e.getMessage());
    }

    return currentMin;
  }

  /**
   * Método requerido por InstanceCountProvider.java para obtener las instancias configuradas.
   */
  public int getServiceInstanceCount(String serviceName, String projectId, String region) {
    return getServiceMinParam(serviceName, projectId, region);
  }

  /**
   * Método requerido por InstanceCountProvider.java para Worker Pools.
   */
  public int getWorkerPoolInstanceCount(String workerpoolName, String projectId, String region) {
    return 0;
  }

  public Instant getServiceLastDeploymentTime(String serviceName, String projectId, String region) {
    return Instant.EPOCH;
  }

  public Instant getWorkerPoolLastDeploymentTime(String workerpoolName, String projectId, String region) {
    return Instant.EPOCH;
  }

  /**
   * Compara la cantidad de instancias recomendadas contra el valor real de --min en Cloud Run.
   * Si NO son iguales, aplica el valor recomendado y envía la orden a Pub/Sub.
   */
  public void updateServiceMinInstances(
      String serviceName, int instances, String projectId, String region)
      throws ExecutionException, InterruptedException, IOException {

    String resourceName = String.format("projects/%s/locations/%s/services/%s", projectId, region, serviceName);

    // Regla de negocio: Si el valor recomendado es 1, se le suma 1 para que pase a 2
    if (instances == 1) {
      instances = instances + 1; 
    }

    // 1. Consultar el parámetro --min actual en Cloud Run e imprimirlo en el LOG
    int currentRealMin = getServiceMinParam(serviceName, projectId, region);

    // 2. Si el valor recomendado es IGUAL al valor real de --min en Cloud Run, no se envía nada.
    if (currentRealMin == instances) {
      logger.atInfo()
          .with(RESOURCE, resourceName)
          .log("[SCALER] Sin cambios requeridos. El parámetro --min actual (%d) coincide con las instancias recomendadas (%d).", 
               currentRealMin, instances);
      lastSentInstancesCache.put(resourceName, instances);
      return;
    }

    // 3. Si NO son iguales (ej: --min en Cloud Run es 2 y las recomendadas son 0), se envía la orden a Pub/Sub
    logger.atInfo()
        .with(RESOURCE, resourceName)
        .log("[SCALER] Discrepancia detectada: --min actual en Cloud Run = %d, instancias recomendadas = %d. Enviando orden de ajuste...", 
             currentRealMin, instances);

    TopicName topicName = TopicName.of(projectId, this.pubSubTopic);
    Publisher publisher = null;

    try {
      publisher = Publisher.newBuilder(topicName).build();

      // Formato de carga útil en JSON para la Cloud Function
      String jsonPayload = String.format(
          "{\"service\":\"%s\", \"project\":\"%s\", \"region\":\"%s\", \"min\":%d, \"min_instances\":%d}",
          serviceName, projectId, region, instances, instances
      );

      PubsubMessage pubsubMessage = PubsubMessage.newBuilder()
          .setData(ByteString.copyFromUtf8(jsonPayload))
          .build();

      publisher.publish(pubsubMessage).get();

      // Actualizar la caché local con el nuevo valor enviado
      lastSentInstancesCache.put(resourceName, instances);

      logger.atInfo()
          .with(RESOURCE, resourceName)
          .log("Órden enviada exitosamente a Pub/Sub (%s) para %s: [--min pasa de %d a %d]", 
               this.pubSubTopic, resourceName, currentRealMin, instances);

    } catch (Exception e) {
      logger.atSevere()
          .with(RESOURCE, resourceName)
          .log("Error al publicar la orden de ajuste de --min=%d a Pub/Sub: %s", instances, e.getMessage());
      throw e;
    } finally {
      if (publisher != null) {
        try {
          publisher.shutdown();
          publisher.awaitTermination(5, TimeUnit.SECONDS);
        } catch (Exception e) {
          // Ignorar excepciones al cerrar recursos del publisher
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
