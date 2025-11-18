/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.operator.cluster.stretch;

import io.fabric8.kubernetes.api.model.HasMetadata;
import io.fabric8.kubernetes.api.model.Service;
import io.fabric8.kubernetes.api.model.ServiceBuilder;
import io.fabric8.kubernetes.api.model.ServicePortBuilder;
import io.strimzi.operator.cluster.operator.resource.ResourceOperatorSupplier;
import io.strimzi.operator.cluster.stretch.spi.StretchNetworkingProvider;
import io.strimzi.operator.common.Reconciliation;
import io.vertx.core.Future;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

/**
 * Simple test implementation of StretchNetworkingProvider for unit tests.
 * 
 * This provider creates ClusterIP services and uses simple DNS names.
 * It's designed to be testable without external dependencies.
 */
public class TestNetworkingProvider implements StretchNetworkingProvider {
    
    private static final Logger LOGGER = LogManager.getLogger(TestNetworkingProvider.class);
    
    private ResourceOperatorSupplier centralSupplier;
    private RemoteResourceOperatorSupplier remoteSupplier;
    private boolean initialized = false;
    private Map<String, String> config;
    
    @Override
    public String getProviderName() {
        return "test-provider";
    }
    
    @Override
    public Future<Void> init(Map<String, String> config,
                             ResourceOperatorSupplier centralSupplier,
                             RemoteResourceOperatorSupplier remoteSupplier) {
        LOGGER.info("Initializing TestNetworkingProvider");
        this.config = config;
        this.centralSupplier = centralSupplier;
        this.remoteSupplier = remoteSupplier;
        this.initialized = true;
        
        // Validate that we have the required suppliers
        if (centralSupplier == null) {
            return Future.failedFuture(new IllegalArgumentException("Central supplier cannot be null"));
        }
        if (remoteSupplier == null) {
            return Future.failedFuture(new IllegalArgumentException("Remote supplier cannot be null"));
        }
        
        LOGGER.info("TestNetworkingProvider initialized successfully");
        return Future.succeededFuture();
    }
    
    @Override
    public Future<List<HasMetadata>> createNetworkingResources(
            Reconciliation reconciliation,
            String namespace,
            String podName,
            String clusterId,
            Map<String, Integer> ports) {
        
        if (!initialized) {
            return Future.failedFuture(new IllegalStateException("Provider not initialized"));
        }
        
        LOGGER.info("Creating networking resources for pod {} in namespace {} on cluster {}", 
                   podName, namespace, clusterId);
        
        List<HasMetadata> resources = new ArrayList<>();
        
        // Create a simple ClusterIP service
        Service service = new ServiceBuilder()
            .withNewMetadata()
                .withName(podName + "-test-svc")
                .withNamespace(namespace)
                .addToLabels("app", "kafka")
                .addToLabels("strimzi.io/cluster", podName.substring(0, podName.lastIndexOf("-kafka-")))
                .addToLabels("test-provider", "true")
            .endMetadata()
            .withNewSpec()
                .withType("ClusterIP")
                .withPorts(ports.entrySet().stream()
                    .map(e -> new ServicePortBuilder()
                        .withName(e.getKey())
                        .withPort(e.getValue())
                        .withNewTargetPort(e.getValue())
                        .build())
                    .collect(Collectors.toList()))
                .withSelector(Map.of("statefulset.kubernetes.io/pod-name", podName))
            .endSpec()
            .build();
        
        resources.add(service);
        
        LOGGER.info("Created {} networking resources for pod {}", resources.size(), podName);
        return Future.succeededFuture(resources);
    }
    
    @Override
    public Future<String> discoverPodEndpoint(
            Reconciliation reconciliation,
            String namespace,
            String serviceName,
            String clusterId,
            String portName) {
        
        if (!initialized) {
            return Future.failedFuture(new IllegalStateException("Provider not initialized"));
        }
        
        // For testing, return a predictable endpoint
        String endpoint = String.format("%s.%s.svc.cluster.local:9091", serviceName, namespace);
        LOGGER.info("Discovered endpoint for service {} in cluster {}: {}", 
                   serviceName, clusterId, endpoint);
        return Future.succeededFuture(endpoint);
    }
    
    @Override
    public String generateServiceDnsName(String namespace, String serviceName, String clusterId) {
        return String.format("%s.%s.svc.cluster.local", serviceName, namespace);
    }
    
    @Override
    public String generatePodDnsName(String namespace, String serviceName, String podName, String clusterId) {
        return String.format("%s.%s.%s.svc.cluster.local", podName, serviceName, namespace);
    }
    
    @Override
    public Future<String> generateAdvertisedListeners(
            Reconciliation reconciliation,
            String namespace,
            String podName,
            String clusterId,
            Map<String, String> listeners) {
        
        if (!initialized) {
            return Future.failedFuture(new IllegalStateException("Provider not initialized"));
        }
        
        // Generate advertised listeners in Kafka format
        String advertisedListeners = listeners.entrySet().stream()
            .map(e -> {
                String listenerName = e.getKey();
                String portName = e.getValue();
                String dnsName = generatePodDnsName(namespace, podName + "-svc", podName, clusterId);
                return String.format("%s://%s:9091", listenerName, dnsName);
            })
            .collect(Collectors.joining(","));
        
        LOGGER.info("Generated advertised listeners for pod {}: {}", podName, advertisedListeners);
        return Future.succeededFuture(advertisedListeners);
    }
    
    @Override
    public Future<String> generateQuorumVoters(
            Reconciliation reconciliation,
            String namespace,
            List<ControllerPodInfo> controllerPods,
            String replicationPortName) {
        
        if (!initialized) {
            return Future.failedFuture(new IllegalStateException("Provider not initialized"));
        }
        
        // Generate quorum voters in KRaft format
        String quorumVoters = controllerPods.stream()
            .map(pod -> {
                String dnsName = generatePodDnsName(namespace, pod.podName() + "-svc", pod.podName(), pod.clusterId());
                return String.format("%d@%s:9091", pod.nodeId(), dnsName);
            })
            .collect(Collectors.joining(","));
        
        LOGGER.info("Generated quorum voters: {}", quorumVoters);
        return Future.succeededFuture(quorumVoters);
    }
    
    @Override
    public Future<List<String>> generateCertificateSans(
            Reconciliation reconciliation,
            String namespace,
            String podName,
            String clusterId) {
        
        if (!initialized) {
            return Future.failedFuture(new IllegalStateException("Provider not initialized"));
        }
        
        // Generate SANs for TLS certificates
        List<String> sans = new ArrayList<>();
        sans.add(generatePodDnsName(namespace, podName + "-svc", podName, clusterId));
        sans.add(generateServiceDnsName(namespace, podName + "-svc", clusterId));
        
        LOGGER.info("Generated certificate SANs for pod {}: {}", podName, sans);
        return Future.succeededFuture(sans);
    }
    
    @Override
    public Future<Void> deleteNetworkingResources(
            Reconciliation reconciliation,
            String namespace,
            String podName,
            String clusterId) {
        
        if (!initialized) {
            return Future.failedFuture(new IllegalStateException("Provider not initialized"));
        }
        
        LOGGER.info("Deleting networking resources for pod {} in namespace {} on cluster {}", 
                   podName, namespace, clusterId);
        
        // In a real implementation, we would delete the service here
        // For testing, we just log and succeed
        return Future.succeededFuture();
    }
    
    /**
     * Check if provider is initialized (for testing).
     */
    public boolean isInitialized() {
        return initialized;
    }
    
    /**
     * Get configuration (for testing).
     */
    public Map<String, String> getConfig() {
        return config;
    }
}
