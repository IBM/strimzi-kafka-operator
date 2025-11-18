/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.operator.cluster.stretch;

import io.fabric8.kubernetes.client.KubernetesClient;
import io.strimzi.api.kafka.model.kafka.Kafka;
import io.strimzi.api.kafka.model.nodepool.KafkaNodePool;
import io.strimzi.operator.common.Reconciliation;
import io.vertx.core.Future;
import io.vertx.core.Vertx;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * Validator for stretch cluster configuration and runtime connectivity.
 * Provides comprehensive validation with user-friendly error messages.
 */
public class StretchClusterValidator {
    /** Logger instance. */
    private static final Logger LOGGER =
        LogManager.getLogger(StretchClusterValidator.class);

    /** Vert.x instance. */
    private final Vertx vertx;
    /** Central cluster ID. */
    private final String centralClusterId;
    /** Set of remote cluster IDs. */
    private final Set<String> remoteClusterIds;

    /**
     * Validation result containing success/failure status and error
     * details.
     */
    public static final class ValidationResult {
        /** Validation status. */
        private final boolean valid;
        /** Error message. */
        private final String errorMessage;
        /** Error code. */
        private final String errorCode;

        private ValidationResult(final boolean validParam,
                                 final String errorMessageParam,
                                 final String errorCodeParam) {
            this.valid = validParam;
            this.errorMessage = errorMessageParam;
            this.errorCode = errorCodeParam;
        }

        /**
         * Create a successful validation result.
         *
         * @return ValidationResult indicating success
         */
        public static ValidationResult success() {
            return new ValidationResult(true, null, null);
        }

        /**
         * Create a failed validation result with error code and message.
         *
         * @param errorCode Error code identifying the type of validation
         *                  failure
         * @param errorMessage Detailed error message describing the
         *                     validation failure
         * @return ValidationResult indicating failure with error details
         */
        public static ValidationResult error(final String errorCode,
                                             final String errorMessage) {
            return new ValidationResult(false, errorMessage, errorCode);
        }

        /**
         * Check if validation passed.
         *
         * @return true if validation passed, false otherwise
         */
        public boolean isValid() {
            return valid;
        }

        /**
         * Get the error message.
         *
         * @return Error message, or null if validation passed
         */
        public String getErrorMessage() {
            return errorMessage;
        }

        /**
         * Get the error code.
         *
         * @return Error code, or null if validation passed
         */
        public String getErrorCode() {
            return errorCode;
        }
    }

    /**
     * Constructor.
     *
     * @param vertxInstance Vert.x instance
     * @param centralClusterIdParam Central cluster ID
     * @param remoteClusterIdsParam Set of remote cluster IDs
     */
    public StretchClusterValidator(final Vertx vertxInstance,
                                    final String centralClusterIdParam,
                                    final Set<String> remoteClusterIdsParam) {
        this.vertx = vertxInstance;
        this.centralClusterId = centralClusterIdParam;
        this.remoteClusterIds = remoteClusterIdsParam;
    }

    /**
     * Validate that operator is properly configured for stretch clusters.
     * Consolidates validation from KafkaClusterCreator.
     *
     * @param isStretchMode Whether stretch mode is enabled on Kafka CR
     * @param isOperatorConfigured Whether operator has stretch cluster env
     *                             vars configured
     * @param kafkaName Name of the Kafka CR (for error messages)
     * @return ValidationResult indicating success or failure with error
     *         details
     */
    public ValidationResult validateOperatorConfiguration(
            final boolean isStretchMode,
            final boolean isOperatorConfigured,
            final String kafkaName) {
        if (isStretchMode && !isOperatorConfigured) {
            return ValidationResult.error(
                "OperatorNotConfigured",
                String.format("Stretch mode is enabled for Kafka cluster "
                    + "'%s', but required environment variables are not "
                    + "properly configured. Required: "
                    + "STRIMZI_REMOTE_KUBE_CONFIG, "
                    + "STRIMZI_CENTRAL_CLUSTER_ID. "
                    + "STRIMZI_STRETCH_PLUGIN_CLASS_NAME"
                    + "STRIMZI_STRETCH_PLUGIN_CLASS_PATH",
                    kafkaName)
            );
        }
        return ValidationResult.success();
    }

    /**
     * Validate Kafka CR and KafkaNodePool configuration.
     * Consolidates validation from StretchClusterReconciler and
     * KafkaClusterCreator.
     *
     * @param kafka Kafka CR
     * @param nodePools List of KafkaNodePool CRs
     * @param isStretchMode Whether stretch mode is enabled (from Kafka CR
     *                      annotation)
     * @return ValidationResult indicating success or failure with error
     *         details
     */
    @SuppressWarnings("checkstyle:CyclomaticComplexity")
    public ValidationResult validateKafkaConfiguration(
            final Kafka kafka,
            final List<KafkaNodePool> nodePools,
            final boolean isStretchMode) {
        String kafkaName = kafka.getMetadata().getName();

        // Check if Kafka CR has stretch annotation
        // The correct annotation is: strimzi.io/enable-stretch-cluster: "true"
        boolean stretchEnabled = kafka.getMetadata().getAnnotations()
            != null
            && "true".equals(kafka.getMetadata().getAnnotations()
                .get("strimzi.io/enable-stretch-cluster"));

        // Validate each KafkaNodePool and collect info
        Set<String> usedClusterIds = new HashSet<>();
        Set<String> poolsWithoutTarget = new HashSet<>();
        Set<String> poolsWithTarget = new HashSet<>();
        Set<String> invalidClusterIds = new HashSet<>();

        for (KafkaNodePool pool : nodePools) {
            String poolName = pool.getMetadata().getName();
            // The correct annotation is: strimzi.io/stretch-cluster-alias
            String targetCluster = pool.getMetadata().getAnnotations()
                != null
                ? pool.getMetadata().getAnnotations()
                    .get("strimzi.io/stretch-cluster-alias")
                : null;

            // Track pools with and without target cluster annotation
            if (targetCluster == null || targetCluster.isEmpty()) {
                poolsWithoutTarget.add(poolName);
            } else {
                poolsWithTarget.add(poolName);

                // Check if target cluster ID is valid
                if (!targetCluster.equals(centralClusterId)
                    && !remoteClusterIds.contains(targetCluster)) {
                    invalidClusterIds.add(targetCluster);
                }

                usedClusterIds.add(targetCluster);
            }
        }

        // PROBLEM 1 FIX: Check if KNPs have stretch annotations but
        // operator is not configured for stretch mode
        if (!isStretchMode && !poolsWithTarget.isEmpty()) {
            return ValidationResult.error(
                "OperatorNotConfigured",
                String.format("Configuration error: The following node "
                    + "pools have 'strimzi.io/stretch-cluster-alias' "
                    + "annotations but the operator is not configured for "
                    + "stretch clusters: %s. Please ensure "
                    + "STRIMZI_REMOTE_KUBE_CONFIG and "
                    + "STRIMZI_CENTRAL_CLUSTER_ID environment variables are "
                    + "set on the operator, or remove the stretch annotations "
                    + "from node pools.",
                    String.join(", ", poolsWithTarget))
            );
        }

        // PROBLEM 1 FIX: Check if KNPs have stretch annotations but Kafka
        // CR doesn't have stretch annotation
        if (isStretchMode && !stretchEnabled && !poolsWithTarget.isEmpty()) {
            return ValidationResult.error(
                "MissingStretchAnnotation",
                String.format("Kafka CR '%s' is missing required "
                    + "annotation 'strimzi.io/enable-stretch-cluster: true' "
                    + "but the following node pools have stretch cluster "
                    + "annotations: %s. Either add the annotation to the "
                    + "Kafka CR or remove 'strimzi.io/stretch-cluster-alias' "
                    + "annotations from all node pools.",
                    kafkaName, String.join(", ", poolsWithTarget))
            );
        }

        // Check if stretch mode is enabled but Kafka CR doesn't have
        // annotation
        if (isStretchMode && !stretchEnabled && poolsWithTarget.isEmpty()) {
            return ValidationResult.error(
                "MissingStretchAnnotation",
                String.format("Kafka CR '%s' is missing required "
                    + "annotation 'strimzi.io/enable-stretch-cluster: true'. "
                    + "Stretch cluster mode must be explicitly enabled on "
                    + "the Kafka CR.", kafkaName)
            );
        }

        // If stretch mode is enabled, ALL pools must have target-cluster
        // annotation (including central cluster pools)
        if (isStretchMode && stretchEnabled && !poolsWithoutTarget.isEmpty()) {
            return ValidationResult.error(
                "MissingTargetCluster",
                String.format("Stretch cluster validation failed: The "
                    + "following node pools are missing required annotation "
                    + "'strimzi.io/stretch-cluster-alias': %s. All node pools "
                    + "must specify a target cluster when stretch mode is "
                    + "enabled.",
                    String.join(", ", poolsWithoutTarget))
            );
        }

        // Report invalid cluster IDs
        if (!invalidClusterIds.isEmpty()) {
            Set<String> validClusterIds = new HashSet<>();
            validClusterIds.add(centralClusterId);
            validClusterIds.addAll(remoteClusterIds);

            return ValidationResult.error(
                "InvalidTargetCluster",
                String.format("Stretch cluster validation failed: The "
                    + "following target cluster IDs are not configured: %s. "
                    + "Valid cluster IDs are: %s. Please check "
                    + "STRIMZI_CENTRAL_CLUSTER_ID and "
                    + "STRIMZI_REMOTE_KUBE_CONFIG.",
                    String.join(", ", invalidClusterIds),
                    String.join(", ", validClusterIds))
            );
        }

        return ValidationResult.success();
    }

    /**
     * Validate runtime connectivity to remote clusters.
     *
     * @param reconciliation Reconciliation context
     * @param clusterClients Map of cluster ID to KubernetesClient
     * @return Future with ValidationResult
     */
    public Future<ValidationResult> validateRuntimeConnectivity(
            final Reconciliation reconciliation,
            final Map<String, KubernetesClient> clusterClients) {

        List<Future<ValidationResult>> futures = new ArrayList<>();

        for (Map.Entry<String, KubernetesClient> entry
                : clusterClients.entrySet()) {
            String clusterId = entry.getKey();
            KubernetesClient client = entry.getValue();

            futures.add(validateClusterConnectivity(reconciliation,
                clusterId, client));
        }

        return Future.join(futures)
            .map(compositeFuture -> {
                // Return first error found, or success if all passed
                for (int i = 0; i < compositeFuture.size(); i++) {
                    ValidationResult result = compositeFuture.resultAt(i);
                    if (!result.isValid()) {
                        return result;
                    }
                }
                return ValidationResult.success();
            });
    }

    /**
     * Validate connectivity to a single cluster.
     *
     * @param reconciliation Reconciliation context
     * @param clusterId Cluster ID
     * @param client KubernetesClient for the cluster
     * @return Future with ValidationResult
     */
    private Future<ValidationResult> validateClusterConnectivity(
            final Reconciliation reconciliation,
            final String clusterId,
            final KubernetesClient client) {

        return vertx.executeBlocking(() -> {
            try {
                // Check API server reachable by getting version
                LOGGER.debug("{}: Validating connectivity to cluster {}",
                    reconciliation, clusterId);
                client.getKubernetesVersion();

                // Check required CRDs exist
                boolean strimziPodSetCrdExists = client.apiextensions().v1()
                    .customResourceDefinitions()
                    .withName("strimzipodsets.core.strimzi.io").get()
                    != null;

                if (!strimziPodSetCrdExists) {
                    return ValidationResult.error(
                        "MissingCRD",
                        String.format("Required CRD "
                            + "'strimzipodsets.core.strimzi.io' not found in "
                            + "cluster '%s'. Please ensure Strimzi CRDs are "
                            + "installed in all target clusters.", clusterId)
                    );
                }

                LOGGER.debug("{}: Cluster {} connectivity validated "
                    + "successfully", reconciliation, clusterId);
                return ValidationResult.success();

            } catch (Exception e) {
                LOGGER.warn("{}: Failed to connect to cluster {}: {}",
                    reconciliation, clusterId, e.getMessage());
                return ValidationResult.error(
                    "ConnectivityError",
                    String.format("Cannot connect to cluster '%s': %s. "
                        + "Please check kubeconfig and network connectivity.",
                        clusterId, e.getMessage())
                );
            }
        });
    }
}
