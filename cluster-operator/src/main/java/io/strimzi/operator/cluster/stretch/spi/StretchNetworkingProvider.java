/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.operator.cluster.stretch.spi;

import io.fabric8.kubernetes.api.model.HasMetadata;
import io.strimzi.operator.cluster.operator.resource.ResourceOperatorSupplier;
import io.strimzi.operator.common.Reconciliation;
import io.vertx.core.Future;

import java.util.List;
import java.util.Map;

/**
 * Service Provider Interface (SPI) for stretch cluster networking implementations.
 *
 * <p>This interface enables pluggable networking strategies for Kafka stretch clusters
 * deployed across multiple Kubernetes clusters. Different implementations provide
 * different approaches to cross-cluster communication.</p>
 *
 * <h2>Plugin-Based Architecture</h2>
 * <p>All networking providers are loaded as external plugins using the same mechanism.
 * This includes Strimzi-maintained providers (NodePort, LoadBalancer) which live in a
 * separate Strimzi repository, as well as third-party providers (e.g., MCS).</p>
 *
 * <p><b>All providers require explicit configuration.</b> There is no default provider.</p>
 *
 * <p><b>Configuration Examples:</b></p>
 * <pre>{@code
 * # NodePort provider (Strimzi-maintained)
 * env:
 * - name: STRIMZI_STRETCH_PLUGIN_CLASS_NAME
 *   value: io.strimzi.operator.cluster.stretch.NodePortNetworkingProvider
 * - name: STRIMZI_STRETCH_PLUGIN_CLASS_PATH
 *   value: /opt/strimzi/plugins/stretch-networking/*
 *
 * # LoadBalancer provider (Strimzi-maintained)
 * env:
 * - name: STRIMZI_STRETCH_PLUGIN_CLASS_NAME
 *   value: io.strimzi.operator.cluster.stretch.LoadBalancerNetworkingProvider
 * - name: STRIMZI_STRETCH_PLUGIN_CLASS_PATH
 *   value: /opt/strimzi/plugins/stretch-networking/*
 *
 * # MCS provider (third-party)
 * env:
 * - name: STRIMZI_STRETCH_PLUGIN_CLASS_NAME
 *   value: com.example.MCSNetworkingProvider
 * - name: STRIMZI_STRETCH_PLUGIN_CLASS_PATH
 *   value: /opt/strimzi/plugins/mcs/*
 * }</pre>
 *
 * <h2>Implementation Requirements</h2>
 * <p>All implementations must:</p>
 * <ul>
 *   <li>Provide a public no-argument constructor</li>
 *   <li>Be thread-safe (methods may be called concurrently)</li>
 *   <li>Be idempotent (methods may be called multiple times)</li>
 *   <li>Handle errors gracefully and return failed Futures with descriptive messages</li>
 *   <li>Clean up resources in {@link #deleteNetworkingResources}</li>
 * </ul>
 *
 * <h2>Lifecycle</h2>
 * <ol>
 *   <li>{@link #init} - Called once during operator startup</li>
 *   <li>{@link #createNetworkingResources} - Called for each pod during reconciliation</li>
 *   <li>{@link #discoverPodEndpoint} - Called to discover endpoints for configuration</li>
 *   <li>{@link #deleteNetworkingResources} - Called during pod/cluster deletion</li>
 * </ol>
 *
 * <h2>Example Implementation</h2>
 * <pre>{@code
 * public class MyNetworkingProvider implements StretchNetworkingProvider {
 *     
 *     @Override
 *     public Future<Void> init(Map<String, String> config,
 *                              Map<String, ResourceOperatorSupplier> clusterSuppliers,
 *                              ResourceOperatorSupplier centralSupplier) {
 *         // Initialize provider with configuration
 *         return Future.succeededFuture();
 *     }
 *     
 *     @Override
 *     public Future<List<HasMetadata>> createNetworkingResources(...) {
 *         // Create services, DNS entries, etc.
 *         return Future.succeededFuture(resources);
 *     }
 *     
 *     @Override
 *     public Future<String> discoverPodEndpoint(...) {
 *         // Return endpoint in format: "host:port"
 *         return Future.succeededFuture("10.0.0.1:9091");
 *     }
 *     
 *     // ... implement other methods
 * }
 * }</pre>
 *
 * @since 0.47.0
 */
public interface StretchNetworkingProvider {

    /**
     * Initialize the provider with configuration and cluster operators.
     * Called once during operator startup before any other methods.
     *
     * <p>Implementations should:
     * <ul>
     *   <li>Validate and store configuration parameters</li>
     *   <li>Store references to cluster operators for later use</li>
     *   <li>Perform any necessary initialization (e.g., validate connectivity)</li>
     *   <li>Return a failed Future if initialization fails</li>
     * </ul>
     *
     * @param config Provider-specific configuration from ConfigMap or environment variables.
     *               May be null if no configuration is provided.
     * @param centralSupplier ResourceOperatorSupplier for the central cluster where the operator runs
     * @param remoteResourceOperatorSupplier Supplier for remote cluster operators
     * @return Future that completes when initialization is done. Return a failed Future if
     *         initialization fails (e.g., invalid configuration, connectivity issues).
     */
    Future<Void> init(
        Map<String, String> config,
        ResourceOperatorSupplier centralSupplier,
        io.strimzi.operator.cluster.stretch.RemoteResourceOperatorSupplier remoteResourceOperatorSupplier
    );

    /**
     * Create networking resources for a Kafka pod.
     * 
     * <p>This method creates all necessary Kubernetes resources to enable cross-cluster
     * communication for a specific Kafka pod. Examples include:</p>
     * <ul>
     *   <li>NodePort: Creates a NodePort Service</li>
     *   <li>LoadBalancer: Creates a LoadBalancer Service</li>
     *   <li>MCS: Creates a Service + ServiceExport</li>
     * </ul>
     *
     * <p><b>Idempotency:</b> This method must be idempotent. It may be called multiple times
     * for the same pod during reconciliation. Implementations should handle this gracefully
     * (e.g., update existing resources instead of failing).</p>
     *
     * <p><b>Thread Safety:</b> This method may be called concurrently for different pods.</p>
     *
     * @param reconciliation Reconciliation context for logging and correlation. Never null.
     * @param namespace Kubernetes namespace where the pod is located. Never null or empty.
     * @param podName Name of the Kafka pod (e.g., "my-cluster-kafka-0"). Never null or empty.
     * @param clusterId Target cluster ID where the pod is located (e.g., "cluster-1"). Never null or empty.
     * @param ports Map of port names to port numbers (e.g., {"replication": 9091, "plain": 9092}).
     *              Never null, but may be empty.
     * @return Future with list of created/updated Kubernetes resources. Return a failed Future
     *         if resource creation fails.
     */
    Future<List<HasMetadata>> createNetworkingResources(
        Reconciliation reconciliation,
        String namespace,
        String podName,
        String clusterId,
        Map<String, Integer> ports
    );

    /**
     * Discover the actual endpoint for a Kafka pod.
     *
     * <p><b>This is the Criticsl method that enables pluggability!</b></p>
     *
     * <p>Each provider returns endpoints in different formats:</p>
     * <ul>
     *   <li><b>NodePort:</b> {@code "10.21.37.21:31001"} (node IP + NodePort)</li>
     *   <li><b>LoadBalancer:</b> {@code "10.21.50.10:9091"} (LoadBalancer IP + port)</li>
     *   <li><b>MCS:</b> {@code "pod.cluster-1.svc.clusterset.local:9091"} (DNS + port)</li>
     * </ul>
     *
     * <p>The returned endpoint will be used in Kafka configuration for:</p>
     * <ul>
     *   <li>{@code advertised.listeners}</li>
     *   <li>{@code controller.quorum.voters}</li>
     *   <li>Inter-broker communication</li>
     * </ul>
     *
     * <p><b>Format:</b> Must return {@code "host:port"} where:</p>
     * <ul>
     *   <li>{@code host} can be an IP address or DNS name</li>
     *   <li>{@code port} is the port number accessible from other clusters</li>
     * </ul>
     *
     * <p><b>Thread Safety:</b> This method may be called concurrently for different pods.</p>
     *
     * @param reconciliation Reconciliation context for logging. Never null.
     * @param namespace Kubernetes namespace where the pod is located. Never null or empty.
     * @param serviceName Service name associated with the pod. Never null or empty.
     * @param clusterId Target cluster ID where the pod is located. Never null or empty.
     * @param portName Port name (e.g., "replication", "controller"). Never null or empty.
     * @return Future with endpoint string in format "host:port". Return a failed Future
     *         if endpoint discovery fails (e.g., service not ready, external IP not assigned).
     */
    Future<String> discoverPodEndpoint(
        Reconciliation reconciliation,
        String namespace,
        String serviceName,
        String clusterId,
        String portName
    );

    /**
     * Generate DNS name for a service in stretch cluster mode.
     *
     * <p>This method generates a DNS name that can be used to access a service
     * from other clusters in the stretch cluster setup.</p>
     *
     * <p>Examples:</p>
     * <ul>
     *   <li>MCS: {@code "my-service.my-ns.svc.clusterset.local"}</li>
     *   <li>NodePort/LoadBalancer: May return the same as serviceName if DNS is not used</li>
     * </ul>
     *
     * @param namespace Kubernetes namespace. Never null or empty.
     * @param serviceName Service name. Never null or empty.
     * @param clusterId Target cluster ID. Never null or empty.
     * @return DNS name that can be used from other clusters. Never null or empty.
     */
    String generateServiceDnsName(String namespace,
                                   String serviceName,
                                   String clusterId);

    /**
     * Generate DNS name for a pod in stretch cluster mode.
     *
     * <p>This method generates a DNS name that can be used to access a specific pod
     * from other clusters in the stretch cluster setup.</p>
     *
     * <p>Examples:</p>
     * <ul>
     *   <li>MCS: {@code "pod-0.my-service.my-ns.svc.clusterset.local"}</li>
     *   <li>NodePort/LoadBalancer: May return the same as podName if DNS is not used</li>
     * </ul>
     *
     * @param namespace Kubernetes namespace. Never null or empty.
     * @param serviceName Service name (used for DNS generation). Never null or empty.
     * @param podName Pod name. Never null or empty.
     * @param clusterId Target cluster ID. Never null or empty.
     * @return DNS name that can be used from other clusters. Never null or empty.
     */
    String generatePodDnsName(String namespace,
                               String serviceName,
                               String podName,
                               String clusterId);

    /**
     * Generate {@code advertised.listeners} configuration for a Kafka broker.
     *
     * <p>This method generates the complete {@code advertised.listeners} configuration
     * string that will be used in the broker's configuration file.</p>
     *
     * <p>Example outputs:</p>
     * <ul>
     *   <li>NodePort: {@code "REPLICATION-9091://10.21.37.21:31001,PLAIN-9092://10.21.37.22:31002"}</li>
     *   <li>LoadBalancer: {@code "REPLICATION-9091://10.21.50.10:9091,PLAIN-9092://10.21.50.11:9092"}</li>
     *   <li>MCS: {@code "REPLICATION-9091://pod.cluster-1.svc.clusterset.local:9091,..."}</li>
     * </ul>
     *
     * <p><b>Format:</b> {@code "LISTENER_NAME-PORT://host:port,..."}</p>
     *
     * @param reconciliation Reconciliation context for logging. Never null.
     * @param namespace Kubernetes namespace. Never null or empty.
     * @param podName Broker pod name. Never null or empty.
     * @param clusterId Cluster ID where the pod is located. Never null or empty.
     * @param listeners Map of listener names to port names (e.g., {"REPLICATION-9091": "replication"}).
     *                  Never null, but may be empty.
     * @return Future with complete {@code advertised.listeners} configuration string.
     *         Return a failed Future if endpoint discovery fails.
     */
    Future<String> generateAdvertisedListeners(
        Reconciliation reconciliation,
        String namespace,
        String podName,
        String clusterId,
        Map<String, String> listeners
    );

    /**
     * Generate {@code controller.quorum.voters} configuration for KRaft mode.
     *
     * <p>This method generates the complete {@code controller.quorum.voters} configuration
     * string that will be used in KRaft mode for controller quorum.</p>
     *
     * <p>Example outputs:</p>
     * <ul>
     *   <li>NodePort: {@code "0@10.21.37.21:31093,1@10.21.37.22:31093"}</li>
     *   <li>LoadBalancer: {@code "0@10.21.50.10:9091,1@10.21.50.11:9091"}</li>
     *   <li>MCS: {@code "0@pod-0.cluster-1.svc.clusterset.local:9091,..."}</li>
     * </ul>
     *
     * <p><b>Format:</b> {@code "nodeId@host:port,..."}</p>
     *
     * @param reconciliation Reconciliation context for logging. Never null.
     * @param namespace Kubernetes namespace. Never null or empty.
     * @param controllerPods List of all controller pod configurations. Never null, but may be empty.
     * @param replicationPortName Port name used for controller replication (e.g., "replication").
     *                            Never null or empty.
     * @return Future with complete {@code controller.quorum.voters} configuration string.
     *         Return a failed Future if endpoint discovery fails.
     */
    Future<String> generateQuorumVoters(
        Reconciliation reconciliation,
        String namespace,
        List<ControllerPodInfo> controllerPods,
        String replicationPortName
    );

    /**
     * Generate additional SANs (Subject Alternative Names) for TLS certificates.
     *
     * <p>Different providers require different SANs to be included in broker certificates:</p>
     * <ul>
     *   <li><b>NodePort:</b> Node IPs (e.g., {@code ["10.21.37.21", "10.21.37.22"]})</li>
     *   <li><b>LoadBalancer:</b> LoadBalancer IPs (e.g., {@code ["10.21.50.10"]})</li>
     *   <li><b>MCS:</b> DNS names (e.g., {@code ["pod.cluster-1.svc.clusterset.local"]})</li>
     * </ul>
     *
     * <p>These SANs will be added to the broker's TLS certificate to ensure clients
     * can verify the certificate when connecting via the external endpoint.</p>
     *
     * @param reconciliation Reconciliation context for logging. Never null.
     * @param namespace Kubernetes namespace. Never null or empty.
     * @param podName Pod name. Never null or empty.
     * @param clusterId Cluster ID. Never null or empty.
     * @return Future with list of additional SANs. May return an empty list if no additional
     *         SANs are needed. Return a failed Future if SAN generation fails.
     */
    Future<List<String>> generateCertificateSans(
        Reconciliation reconciliation,
        String namespace,
        String podName,
        String clusterId
    );

    /**
     * Delete networking resources for a pod.
     *
     * <p>This method deletes all networking resources that were created by
     * {@link #createNetworkingResources} for the specified pod.</p>
     *
     * <p><b>Idempotency:</b> This method must be idempotent. It may be called multiple times
     * for the same pod. Implementations should handle this gracefully (e.g., ignore
     * "not found" errors).</p>
     *
     * <p><b>Called during:</b></p>
     * <ul>
     *   <li>Pod deletion</li>
     *   <li>Cluster scale-down</li>
     *   <li>Kafka cluster deletion</li>
     * </ul>
     *
     * <p><b>Thread Safety:</b> This method may be called concurrently for different pods.</p>
     *
     * @param reconciliation Reconciliation context for logging. Never null.
     * @param namespace Kubernetes namespace. Never null or empty.
     * @param podName Pod name. Never null or empty.
     * @param clusterId Cluster ID. Never null or empty.
     * @return Future that completes when resources are deleted. Return a failed Future
     *         only if deletion fails in a way that requires operator attention (e.g.,
     *         permission denied). "Not found" errors should be ignored.
     */
    Future<Void> deleteNetworkingResources(
        Reconciliation reconciliation,
        String namespace,
        String podName,
        String clusterId
    );

    /**
     * Get provider name for logging and metrics.
     *
     * <p>This name is used in:</p>
     * <ul>
     *   <li>Log messages (e.g., "Using NodePort networking provider")</li>
     *   <li>Metrics labels</li>
     *   <li>Status reporting</li>
     * </ul>
     *
     * <p><b>Naming Convention:</b> Use lowercase with hyphens (e.g., "nodeport", "load-balancer", "mcs").</p>
     *
     * @return Provider identifier. Never null or empty.
     */
    String getProviderName();

    /**
     * Information about a controller pod for {@code controller.quorum.voters} generation.
     *
     * @param nodeId Kafka node ID (must be unique across the cluster)
     * @param podName Pod name (e.g., "my-cluster-kafka-0")
     * @param clusterId Cluster ID where the pod is located (e.g., "cluster-1")
     */
    record ControllerPodInfo(int nodeId, String podName, String clusterId) { }
}
