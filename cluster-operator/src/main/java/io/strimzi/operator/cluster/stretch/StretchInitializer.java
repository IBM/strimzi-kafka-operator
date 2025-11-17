/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.operator.cluster.stretch;

import io.fabric8.kubernetes.client.KubernetesClient;
import io.micrometer.core.instrument.composite.CompositeMeterRegistry;
import io.strimzi.operator.cluster.ClusterOperatorConfig;
import io.strimzi.operator.cluster.PlatformFeaturesAvailability;
import io.strimzi.operator.cluster.RemoteClientSupplier;
import io.strimzi.operator.cluster.model.DnsNameGenerator;
import io.strimzi.operator.cluster.operator.resource.ResourceOperatorSupplier;
import io.strimzi.operator.cluster.stretch.spi.StretchNetworkingProvider;
import io.strimzi.operator.common.MicrometerMetricsProvider;
import io.vertx.core.Future;
import io.vertx.core.Vertx;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Initializer for stretch cluster functionality.
 * This class encapsulates all stretch-cluster-specific initialization logic
 * to keep Main.java focused on core operator startup.
 */
public class StretchInitializer {
    private static final Logger LOGGER = LogManager.getLogger(StretchInitializer.class);

    /**
     * Result of stretch initialization containing all necessary components.
     */
    public static class InitializationResult {
        private final Map<String, PlatformFeaturesAvailability> remotePfas;
        private final RemoteResourceOperatorSupplier remoteResourceOperatorSupplier;
        private final boolean remoteKubeConfigsValid;
        private final boolean envConfigsValid;

        /**
         * Constructor.
         *
         * @param remotePfas Platform features availability for remote clusters
         * @param remoteResourceOperatorSupplier Remote resource operator supplier
         * @param envConfigsValid Cluster operator environment configs valid
         * @param remoteKubeConfigsValid Remote Kuberenetes clients did not expire
         */
        public InitializationResult(
                Map<String, PlatformFeaturesAvailability> remotePfas,
                RemoteResourceOperatorSupplier remoteResourceOperatorSupplier,
                boolean envConfigsValid,
                boolean remoteKubeConfigsValid) {
            this.remotePfas = remotePfas;
            this.remoteResourceOperatorSupplier = remoteResourceOperatorSupplier;
            this.envConfigsValid = envConfigsValid;
            this.remoteKubeConfigsValid = remoteKubeConfigsValid;
        }

        /**
         * Gets the remote platform features availability map.
         *
         * @return Map of cluster ID to PlatformFeaturesAvailability
         */
        public Map<String, PlatformFeaturesAvailability> getRemotePfas() {
            return remotePfas;
        }

        /**
         * Gets the remote resource operator supplier.
         *
         * @return RemoteResourceOperatorSupplier or null if stretch not configured
         */
        public RemoteResourceOperatorSupplier getRemoteResourceOperatorSupplier() {
            return remoteResourceOperatorSupplier;
        }

        /**
         * Gets the the result of remote kube configs valid
         *
         * @return value of remoteKubeConfigsValid
         */
        public boolean remoteKubeConfigsValid() {
            return remoteKubeConfigsValid;
        }

        /**
         * Gets the the result of env configs valid
         *
         * @return value of envconfigs valid
         */
        public boolean envConfigsValid() {
            return envConfigsValid;
        }
    }

    /**
     * Initialize stretch cluster functionality.
     * This includes:
     * 1. Creating PlatformFeaturesAvailability for remote clusters
     * 2. Creating RemoteResourceOperatorSupplier
     * 3. Initializing the stretch networking provider
     *
     * @param config Cluster operator configuration
     * @param vertx Vertx instance
     * @param client Kubernetes client for central cluster
     * @param remoteClientSupplier Supplier for remote cluster clients
     * @param centralPfa Platform features availability for central cluster
     * @return Future containing InitializationResult
     */
    public static Future<InitializationResult> initialize(
            ClusterOperatorConfig config,
            Vertx vertx,
            KubernetesClient client,
            RemoteClientSupplier remoteClientSupplier,
            PlatformFeaturesAvailability centralPfa) {

        // Check if stretch is configured
        if (!config.isStretchClusterConfigurationValid()) {
            LOGGER.debug("Stretch cluster configuration not valid. Skipping stretch initialization.");
            return Future.succeededFuture(new InitializationResult(new HashMap<>(), null, false, false));
        } 

        StretchClusterValidator validator = new StretchClusterValidator(vertx, config.getCentralClusterId(), config.getRemoteClusters().keySet());

        
        
        return validator
            .validateRuntimeConnectivity(remoteClientSupplier.getRemoteClients())
            .compose(result -> {
                if (result.isValid()) {
                    LOGGER.info("Initializing stretch cluster functionality...");
                    // Step 1: Create PlatformFeaturesAvailability for remote clusters
                    return createRemotePlatformFeaturesAvailability(vertx, remoteClientSupplier)
                        .compose(remotePfas -> {
                            RemoteResourceOperatorSupplier remoteResourceOperatorSupplier =
                                initializeNetworkingProvider(config, vertx, client, remoteClientSupplier, centralPfa, remotePfas);
                            // Step 2: Initialize networking provider and create RemoteResourceOperatorSupplier
                            return Future.succeededFuture(new InitializationResult(remotePfas, remoteResourceOperatorSupplier, true, true));
                        });
                } else {
                    LOGGER.error("Kube config files are invalid. Cannot reconcile stretched kafka cluster");
                    return Future.succeededFuture(new InitializationResult(new HashMap<>(), null, true, false));
                }
            });
    }

    /**
     * Creates PlatformFeaturesAvailability for all configured remote clusters.
     *
     * @param vertx Vert.x instance
     * @param remoteClientSupplier RemoteClientSupplier with clients for each remote cluster
     * @return Future containing a map of cluster ID to PlatformFeaturesAvailability
     */
    private static Future<Map<String, PlatformFeaturesAvailability>> createRemotePlatformFeaturesAvailability(
            Vertx vertx,
            RemoteClientSupplier remoteClientSupplier) {

        Map<String, PlatformFeaturesAvailability> remotePfas = new HashMap<>();
        List<Future<PlatformFeaturesAvailability>> pfaFutures = new ArrayList<>();

        for (Map.Entry<String, KubernetesClient> targetClusterClient : remoteClientSupplier.getRemoteClients().entrySet()) {
            String clusterId = targetClusterClient.getKey();
            KubernetesClient clusterClient = targetClusterClient.getValue();

            LOGGER.debug("Creating PlatformFeaturesAvailability for remote cluster '{}'", clusterId);

            pfaFutures.add(PlatformFeaturesAvailability
                .create(vertx, clusterClient, true)
                .recover(error -> handleRemoteClusterConnectionError(clusterId, error))
                .compose(pfaResult -> {
                    // Only add to map if connection was successful (not null)
                    if (pfaResult != null) {
                        remotePfas.put(clusterId, pfaResult);
                        LOGGER.info("PlatformFeaturesAvailability created for remote cluster '{}'", clusterId);
                    } else {
                        LOGGER.warn("Skipping remote cluster '{}' due to connection failure", clusterId);
                    }
                    return Future.succeededFuture(pfaResult);
                }));
        }

        return Future.join(pfaFutures)
            .map(x -> remotePfas);
    }

    /**
     * Initialize stretch networking provider and create RemoteResourceOperatorSupplier.
     *
     * @param config Cluster operator configuration
     * @param vertx Vertx instance
     * @param client Kubernetes client for central cluster
     * @param remoteClientSupplier Supplier for remote cluster clients
     * @param centralPfa Platform features availability for central cluster
     * @param remotePfas Platform features availability for remote clusters
     * @return RemoteResourceOperatorSupplier if successful, null otherwise
     */
    private static RemoteResourceOperatorSupplier initializeNetworkingProvider(
            ClusterOperatorConfig config,
            Vertx vertx,
            KubernetesClient client,
            RemoteClientSupplier remoteClientSupplier,
            PlatformFeaturesAvailability centralPfa,
            Map<String, PlatformFeaturesAvailability> remotePfas) {

        try {
            // Load provider configuration from ConfigMap if specified
            Map<String, String> providerConfig = new HashMap<>();
            String configMapName = config.getStretchNetworkConfigMap();
            if (configMapName != null && !configMapName.isEmpty()) {
                LOGGER.info("Provider configuration from ConfigMap '{}' will be loaded", configMapName);
            }

            // Create RemoteResourceOperatorSupplier for remote clusters
            RemoteResourceOperatorSupplier remoteResourceOperatorSupplier = new RemoteResourceOperatorSupplier(
                vertx,
                client,
                remoteClientSupplier,
                remotePfas,
                config.getOperatorName(),
                config.getCentralClusterId()
            );

            // Create central cluster supplier (simplified - no remote clusters for central)
            ResourceOperatorSupplier centralSupplier = new ResourceOperatorSupplier(
                vertx,
                client,
                new MicrometerMetricsProvider(new CompositeMeterRegistry()),
                centralPfa,
                config.getOperatorName()
            );

            // Create and initialize the networking provider
            StretchNetworkingProvider provider = StretchNetworkingProviderFactory.create(
                config,
                providerConfig,
                centralSupplier,
                remoteResourceOperatorSupplier
            );

            // Set the provider globally in DnsNameGenerator
            DnsNameGenerator.setStretchProvider(provider);

            LOGGER.info("Stretch networking provider '{}' initialized successfully", provider.getProviderName());

            return remoteResourceOperatorSupplier;

        } catch (Exception e) {
            LOGGER.error("Failed to initialize stretch networking provider. Stretch cluster functionality may not work correctly.", e);
            // Don't fail startup - allow operator to start but stretch clusters won't work
            return null;
        }
    }

    /**
     * Handles connection errors when attempting to connect to remote clusters.
     * Provides specific error messages for authentication failures (expired tokens).
     * Returns null instead of failing to allow the operator to continue running.
     *
     * @param clusterId The ID of the remote cluster that failed to connect
     * @param error The error that occurred during connection
     * @return A succeeded Future with null value to skip this cluster
     */
    private static Future<PlatformFeaturesAvailability> handleRemoteClusterConnectionError(String clusterId, Throwable error) {
        if (isAuthenticationError(error)) {
            LOGGER.error("Failed to connect to remote cluster '{}'. " +
                "The kubeconfig secret appears to be invalid or expired. " +
                "Please update the secret referenced in STRIMZI_REMOTE_KUBE_CONFIG with valid credentials. " +
                "Stretch cluster functionality will be disabled for this cluster until credentials are fixed. " +
                "Error: {}", clusterId, error.getMessage());
            
            // Return null to skip this cluster but continue operator startup
            return Future.succeededFuture(null);
        }
        
        LOGGER.error("Failed to create PlatformFeaturesAvailability for remote cluster '{}'. " +
            "Stretch cluster functionality will be disabled for this cluster. " +
            "Error: {}", clusterId, error.getMessage());
        
        // Return null to skip this cluster but continue operator startup
        return Future.succeededFuture(null);
    }

    /**
     * Checks if an error is related to authentication or authorization failures.
     * This typically indicates expired or invalid credentials.
     *
     * @param error The error to check
     * @return true if this is an authentication/authorization error, false otherwise
     */
    private static boolean isAuthenticationError(Throwable error) {
        if (error == null || error.getMessage() == null) {
            return false;
        }

        String message = error.getMessage();
        return message.contains("Unauthorized") ||
               message.contains("401") ||
               message.contains("Forbidden") ||
               message.contains("403");
    }
}
