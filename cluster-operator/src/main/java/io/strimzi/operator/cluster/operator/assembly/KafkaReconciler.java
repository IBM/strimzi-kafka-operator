/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.operator.cluster.operator.assembly;

import io.fabric8.kubernetes.api.model.ConfigMap;
import io.fabric8.kubernetes.api.model.ConfigMapBuilder;
import io.fabric8.kubernetes.api.model.HasMetadata;
import io.fabric8.kubernetes.api.model.LocalObjectReference;
import io.fabric8.kubernetes.api.model.Node;
import io.fabric8.kubernetes.api.model.OwnerReference;
import io.fabric8.kubernetes.api.model.OwnerReferenceBuilder;
import io.fabric8.kubernetes.api.model.PersistentVolumeClaim;
import io.fabric8.kubernetes.api.model.Pod;
import io.fabric8.kubernetes.api.model.Secret;
import io.fabric8.kubernetes.api.model.ServiceAccount;
import io.fabric8.kubernetes.api.model.rbac.ClusterRoleBinding;
import io.fabric8.kubernetes.client.KubernetesClient;
import io.strimzi.api.kafka.model.common.Condition;
import io.strimzi.api.kafka.model.kafka.Kafka;
import io.strimzi.api.kafka.model.kafka.KafkaMetadataState;
import io.strimzi.api.kafka.model.kafka.KafkaResources;
import io.strimzi.api.kafka.model.kafka.KafkaStatus;
import io.strimzi.api.kafka.model.kafka.UsedNodePoolStatus;
import io.strimzi.api.kafka.model.kafka.UsedNodePoolStatusBuilder;
import io.strimzi.api.kafka.model.kafka.cruisecontrol.KafkaAutoRebalanceStatus;
import io.strimzi.api.kafka.model.kafka.listener.GenericKafkaListener;
import io.strimzi.api.kafka.model.kafka.listener.ListenerAddress;
import io.strimzi.api.kafka.model.kafka.listener.ListenerAddressBuilder;
import io.strimzi.api.kafka.model.kafka.listener.ListenerStatus;
import io.strimzi.api.kafka.model.kafka.listener.ListenerStatusBuilder;
import io.strimzi.api.kafka.model.kafka.quotas.QuotasPluginKafka;
import io.strimzi.api.kafka.model.nodepool.KafkaNodePool;
import io.strimzi.api.kafka.model.nodepool.KafkaNodePoolBuilder;
import io.strimzi.api.kafka.model.nodepool.KafkaNodePoolList;
import io.strimzi.api.kafka.model.nodepool.KafkaNodePoolStatus;
import io.strimzi.api.kafka.model.nodepool.KafkaNodePoolStatusBuilder;
import io.strimzi.api.kafka.model.podset.StrimziPodSet;
import io.strimzi.api.kafka.model.podset.StrimziPodSetBuilder;
import io.strimzi.operator.cluster.ClusterOperatorConfig;
import io.strimzi.operator.cluster.PlatformFeaturesAvailability;
import io.strimzi.operator.cluster.model.CertUtils;
import io.strimzi.operator.cluster.model.ClusterCa;
import io.strimzi.operator.cluster.model.ImagePullPolicy;
import io.strimzi.operator.cluster.model.KafkaCluster;
import io.strimzi.operator.cluster.model.KafkaConfiguration;
import io.strimzi.operator.cluster.model.KafkaPool;
import io.strimzi.operator.cluster.model.ListenersUtils;
import io.strimzi.operator.cluster.model.MetricsAndLogging;
import io.strimzi.operator.cluster.model.NodeRef;
import io.strimzi.operator.cluster.model.PodSetUtils;
import io.strimzi.operator.cluster.model.RestartReason;
import io.strimzi.operator.cluster.model.RestartReasons;
import io.strimzi.operator.cluster.operator.resource.ConcurrentDeletionException;
import io.strimzi.operator.cluster.operator.resource.KafkaAgentClientProvider;
import io.strimzi.operator.cluster.operator.resource.KafkaRoller;
import io.strimzi.operator.cluster.operator.resource.ResourceOperatorSupplier;
import io.strimzi.operator.cluster.operator.resource.events.KubernetesRestartEventPublisher;
import io.strimzi.operator.cluster.operator.resource.kubernetes.ClusterRoleBindingOperator;
import io.strimzi.operator.cluster.operator.resource.kubernetes.ConfigMapOperator;
import io.strimzi.operator.cluster.operator.resource.kubernetes.CrdOperator;
import io.strimzi.operator.cluster.operator.resource.kubernetes.IngressOperator;
import io.strimzi.operator.cluster.operator.resource.kubernetes.NetworkPolicyOperator;
import io.strimzi.operator.cluster.operator.resource.kubernetes.NodeOperator;
import io.strimzi.operator.cluster.operator.resource.kubernetes.PodDisruptionBudgetOperator;
import io.strimzi.operator.cluster.operator.resource.kubernetes.PodOperator;
import io.strimzi.operator.cluster.operator.resource.kubernetes.PvcOperator;
import io.strimzi.operator.cluster.operator.resource.kubernetes.RouteOperator;
import io.strimzi.operator.cluster.operator.resource.kubernetes.SecretOperator;
import io.strimzi.operator.cluster.operator.resource.kubernetes.ServiceAccountOperator;
import io.strimzi.operator.cluster.operator.resource.kubernetes.ServiceOperator;
import io.strimzi.operator.cluster.operator.resource.kubernetes.StorageClassOperator;
import io.strimzi.operator.cluster.operator.resource.kubernetes.StrimziPodSetOperator;
import io.strimzi.operator.common.AdminClientProvider;
import io.strimzi.operator.common.Annotations;
import io.strimzi.operator.common.BackOff;
import io.strimzi.operator.common.Reconciliation;
import io.strimzi.operator.common.ReconciliationLogger;
import io.strimzi.operator.common.Util;
import io.strimzi.operator.common.auth.TlsPemIdentity;
import io.strimzi.operator.common.model.Ca;
import io.strimzi.operator.common.model.ClientsCa;
import io.strimzi.operator.common.model.Labels;
import io.strimzi.operator.common.model.NodeUtils;
import io.strimzi.operator.common.model.StatusDiff;
import io.strimzi.operator.common.model.StatusUtils;
import io.strimzi.operator.common.operator.resource.ReconcileResult;
import io.vertx.core.Future;
import io.vertx.core.Promise;
import io.vertx.core.Vertx;
import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.common.KafkaException;

import java.time.Clock;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ExecutionException;
import java.util.function.Function;
import java.util.stream.Collectors;

import static io.strimzi.operator.cluster.model.KafkaCluster.ANNO_STRIMZI_IO_KAFKA_VERSION;
import static io.strimzi.operator.common.Annotations.ANNO_STRIMZI_SERVER_CERT_HASH;

/**
 * Class used for reconciliation of Kafka. This class contains both the steps of the Kafka
 * reconciliation pipeline (although the steps for listener reconciliation are outsourced to the KafkaListenerReconciler)
 * and is also used to store the state between them.
 */
@SuppressWarnings({"checkstyle:ClassFanOutComplexity", "checkstyle:ClassDataAbstractionCoupling"})
public class KafkaReconciler {
    private static final ReconciliationLogger LOGGER = ReconciliationLogger.create(KafkaReconciler.class.getName());

    // Various settings
    private final long operationTimeoutMs;
    private final boolean isNetworkPolicyGeneration;
    private final boolean isPodDisruptionBudgetGeneration;
    private final List<String> maintenanceWindows;
    private final String operatorNamespace;
    private final Labels operatorNamespaceLabels;
    private final PlatformFeaturesAvailability pfa;
    private final ImagePullPolicy imagePullPolicy;
    private final List<LocalObjectReference> imagePullSecrets;

    // Objects used during the reconciliation
    /* test */ final Reconciliation reconciliation;
    private final Kafka kafkaCr; // Kafka custom resource
    private final KafkaCluster kafka;
    private final List<KafkaNodePool> kafkaNodePoolCrs;
    private final ClusterCa clusterCa;
    private final ClientsCa clientsCa;

    // Tools for operating and managing various resources
    private final Vertx vertx;
    private final StrimziPodSetOperator strimziPodSetOperator;
    private final SecretOperator secretOperator;
    private final ServiceAccountOperator serviceAccountOperator;
    private final ServiceOperator serviceOperator;
    private final PvcOperator pvcOperator;
    private final StorageClassOperator storageClassOperator;
    private final ConfigMapOperator configMapOperator;
    private final NetworkPolicyOperator networkPolicyOperator;
    private final PodDisruptionBudgetOperator podDisruptionBudgetOperator;
    private final PodOperator podOperator;
    private final ClusterRoleBindingOperator clusterRoleBindingOperator;
    private final RouteOperator routeOperator;
    private final IngressOperator ingressOperator;
    private final NodeOperator nodeOperator;
    private final CrdOperator<KubernetesClient, KafkaNodePool, KafkaNodePoolList> kafkaNodePoolOperator;
    private final KubernetesRestartEventPublisher eventsPublisher;
    private final AdminClientProvider adminClientProvider;
    private final KafkaAgentClientProvider kafkaAgentClientProvider;

    // Stretch cluster support (optional - only set when stretch mode is enabled)
    /* test */ Map<String, ServiceAccountOperator> stretchServiceAccountOperators;
    /* test */ Map<String, StrimziPodSetOperator> stretchStrimziPodsetOperators;
    /* test */ Map<String, PodOperator> stretchPodOperators;
    /* test */ Map<String, SecretOperator> stretchSecretOperators;
    /* test */ Map<String, ConfigMapOperator> stretchConfigMapOperators;
    /* test */ Map<String, PvcOperator> stretchPvcOperators;
    /* test */ Map<String, StorageClassOperator> stretchStorageClassOperators;
    /* test */ Map<String, ClusterRoleBindingOperator> stretchClusterRoleBindingOperators;
    /* test */ Map<String, ServiceOperator> stretchServiceOperators;
    /* test */ Map<String, RouteOperator> stretchRouteOperators;
    /* test */ Map<String, IngressOperator> stretchIngressOperators;
    /* test */ io.strimzi.operator.cluster.stretch.spi.StretchNetworkingProvider networkingProvider;
    /* test */ io.strimzi.operator.cluster.operator.resource.ResourceOperatorSupplier centralSupplier;
    /* test */ io.strimzi.operator.cluster.stretch.RemoteResourceOperatorSupplier remoteSupplier;

    // State of the reconciliation => these objects might change during the reconciliation (the collection objects are
    // marked as final, but their contents is modified during the reconciliation)
    private final Set<String> fsResizingRestartRequest = new HashSet<>();

    private final Map<Integer, String> brokerConfigurationHash = new HashMap<>();
    private final Map<Integer, String> kafkaServerCertificateHash = new HashMap<>();
    private final List<String> secretsToDelete = new ArrayList<>();
    /* test */ TlsPemIdentity coTlsPemIdentity;
    /* test */ KafkaListenersReconciler.ReconciliationResult listenerReconciliationResults; // Result of the listener reconciliation with the listener details

    private final KafkaAutoRebalanceStatus kafkaAutoRebalanceStatus;
    private final Map<String, List<String>> stretchSecretsToDelete = new HashMap<>();
    private final Map<String, String> gcConfigMapUids = new HashMap<>(); // Store GC ConfigMap UIDs by cluster ID

    // Stretch cluster state
    private io.strimzi.operator.cluster.stretch.StretchClusterValidator validator;

    /* test */ boolean isStretchMode;
    /* test */ String stretchCentralClusterId;
    /* test */ Set<String> remoteClusterIds;
    /* test */ List<String> clusterIds;
    /* test */ Map<String, PlatformFeaturesAvailability> remotePfas;

    /**
     * Constructs the Kafka reconciler
     *
     * @param reconciliation            Reconciliation marker
     * @param kafkaCr                   The Kafka custom resource
     * @param nodePools                 List of KafkaNodePool resources belonging to this cluster
     * @param kafka                     Kafka cluster instance
     * @param clusterCa                 The Cluster CA instance
     * @param clientsCa                 The Clients CA instance
     * @param config                    Cluster Operator Configuration
     * @param supplier                  Supplier with Kubernetes Resource Operators
     * @param pfa                       PlatformFeaturesAvailability describing the environment we run in
     * @param vertx                     Vert.x instance
     */
    public KafkaReconciler(
            Reconciliation reconciliation,
            Kafka kafkaCr,
            List<KafkaNodePool> nodePools,
            KafkaCluster kafka,
            ClusterCa clusterCa,
            ClientsCa clientsCa,
            ClusterOperatorConfig config,
            ResourceOperatorSupplier supplier,
            PlatformFeaturesAvailability pfa,
            Vertx vertx
    ) {
        this.reconciliation = reconciliation;
        this.kafkaCr = kafkaCr;
        this.vertx = vertx;
        this.operationTimeoutMs = config.getOperationTimeoutMs();
        this.kafkaNodePoolCrs = nodePools;
        this.kafka = kafka;

        this.clusterCa = clusterCa;
        this.clientsCa = clientsCa;
        this.maintenanceWindows = kafkaCr.getSpec().getMaintenanceTimeWindows();
        this.operatorNamespace = config.getOperatorNamespace();
        this.operatorNamespaceLabels = config.getOperatorNamespaceLabels();
        this.isNetworkPolicyGeneration = config.isNetworkPolicyGeneration();
        this.pfa = pfa;
        this.imagePullPolicy = config.getImagePullPolicy();
        this.imagePullSecrets = config.getImagePullSecrets();
        this.isPodDisruptionBudgetGeneration = config.isPodDisruptionBudgetGeneration();
        this.kafkaAutoRebalanceStatus = kafkaCr.getStatus() != null ? kafkaCr.getStatus().getAutoRebalance() : null;

        this.strimziPodSetOperator = supplier.strimziPodSetOperator;
        this.secretOperator = supplier.secretOperations;
        this.serviceAccountOperator = supplier.serviceAccountOperations;
        this.serviceOperator = supplier.serviceOperations;
        this.pvcOperator = supplier.pvcOperations;
        this.storageClassOperator = supplier.storageClassOperations;
        this.configMapOperator = supplier.configMapOperations;
        this.networkPolicyOperator = supplier.networkPolicyOperator;
        this.podDisruptionBudgetOperator = supplier.podDisruptionBudgetOperator;
        this.podOperator = supplier.podOperations;
        this.clusterRoleBindingOperator = supplier.clusterRoleBindingOperator;
        this.routeOperator = supplier.routeOperations;
        this.ingressOperator = supplier.ingressOperations;
        this.nodeOperator = supplier.nodeOperator;
        this.kafkaNodePoolOperator = supplier.kafkaNodePoolOperator;
        this.eventsPublisher = supplier.restartEventsPublisher;

        this.adminClientProvider = supplier.adminClientProvider;
        this.kafkaAgentClientProvider = supplier.kafkaAgentClientProvider;
    }

    /**
     * Sets up stretch cluster configuration. This method should be called after construction
     * if stretch cluster mode is enabled.
     *
     * @param remoteResourceOperatorSupplier    Supplier for remote cluster operators
     * @param networkingProvider                Networking provider for stretch clusters
     * @param centralClusterId                  ID of the central cluster
     * @param centralSupplier                   Supplier for central cluster operators
     * @param remotePfas                        Remote platform features availabilities
     */
    public void setupStretchClusterConfig(
            io.strimzi.operator.cluster.stretch.RemoteResourceOperatorSupplier remoteResourceOperatorSupplier,
            io.strimzi.operator.cluster.stretch.spi.StretchNetworkingProvider networkingProvider,
            String centralClusterId,
            io.strimzi.operator.cluster.operator.resource.ResourceOperatorSupplier centralSupplier,
            Map<String, PlatformFeaturesAvailability> remotePfas) {

        // Get target clusters from node pools
        this.remoteClusterIds = new HashSet<>();
        this.clusterIds = new ArrayList<>();
        this.remotePfas = remotePfas;
        clusterIds.add(centralClusterId);

        for (KafkaNodePool pool : kafkaNodePoolCrs) {
            if (pool.getMetadata().getAnnotations() != null) {
                String alias = pool.getMetadata().getAnnotations()
                    .get("strimzi.io/stretch-cluster-alias");
                if (alias != null && !alias.isEmpty() && !clusterIds.contains(alias)) {
                    clusterIds.add(alias);
                    remoteClusterIds.add(alias);
                }
            }
        }

        this.validator = new io.strimzi.operator.cluster.stretch.StretchClusterValidator(vertx, centralClusterId, remoteClusterIds);


        // Store suppliers for use in stretch listener reconciler
        this.centralSupplier = centralSupplier;
        this.remoteSupplier = remoteResourceOperatorSupplier;

        // Set up operator maps for remote clusters
        this.stretchServiceAccountOperators = new HashMap<>();
        this.stretchStrimziPodsetOperators = new HashMap<>();
        this.stretchPodOperators = new HashMap<>();
        this.stretchSecretOperators = new HashMap<>();
        this.stretchConfigMapOperators = new HashMap<>();
        this.stretchPvcOperators = new HashMap<>();
        this.stretchStorageClassOperators = new HashMap<>();
        this.stretchClusterRoleBindingOperators = new HashMap<>();
        this.stretchServiceOperators = new HashMap<>();
        this.stretchRouteOperators = new HashMap<>();
        this.stretchIngressOperators = new HashMap<>();

        // Populate maps from remote supplier
        for (String clusterId : remoteResourceOperatorSupplier.remoteResourceOperators.keySet()) {
            io.strimzi.operator.cluster.operator.resource.ResourceOperatorSupplier supplier =
                remoteResourceOperatorSupplier.get(clusterId);

            stretchServiceAccountOperators.put(clusterId, supplier.serviceAccountOperations);
            stretchStrimziPodsetOperators.put(clusterId, supplier.strimziPodSetOperator);
            stretchPodOperators.put(clusterId, supplier.podOperations);
            stretchSecretOperators.put(clusterId, supplier.secretOperations);
            stretchConfigMapOperators.put(clusterId, supplier.configMapOperations);
            stretchPvcOperators.put(clusterId, supplier.pvcOperations);
            stretchStorageClassOperators.put(clusterId, supplier.storageClassOperations);
            stretchClusterRoleBindingOperators.put(clusterId, supplier.clusterRoleBindingOperator);
            stretchServiceOperators.put(clusterId, supplier.serviceOperations);
            stretchRouteOperators.put(clusterId, supplier.routeOperations);
            stretchIngressOperators.put(clusterId, supplier.ingressOperations);
        }

        this.networkingProvider = networkingProvider;
        this.isStretchMode = true;
        this.stretchCentralClusterId = centralClusterId;
    }

        /**
         * Handle validation errors by updating Kafka CR status with error details.
         *
         * @param reconciliation Reconciliation context
         * @param kafka Kafka CR
         * @param result Validation result containing error details
         * @return Future with KafkaStatus containing error condition
         */
    private Future<Void> handleValidationError(
            KafkaStatus status,
            io.strimzi.operator.cluster.stretch.StretchClusterValidator.ValidationResult result) {

        LOGGER.errorOp("{}: Stretch cluster validation failed: {}", 
            reconciliation, result.getErrorMessage());

        // Add Ready: False condition
        io.strimzi.api.kafka.model.common.Condition readyCondition = 
            new io.strimzi.api.kafka.model.common.ConditionBuilder()
                .withType("Ready")
                .withStatus("False")
                .withReason(result.getErrorCode())
                .withMessage(result.getErrorMessage())
                .withLastTransitionTime(StatusUtils.iso8601Now())
                .build();

        // Add StretchCluster: False condition to indicate stretch cluster validation failed
        io.strimzi.api.kafka.model.common.Condition stretchCondition = 
            new io.strimzi.api.kafka.model.common.ConditionBuilder()
                .withType("StretchCluster")
                .withStatus("False")
                .withReason("InvalidStretchConfiguration")
                .withMessage("Stretch cluster validation failed: " + result.getErrorMessage())
                .withLastTransitionTime(StatusUtils.iso8601Now())
                .build();

        status.setConditions(List.of(readyCondition, stretchCondition));
        status.setObservedGeneration(kafkaCr.getMetadata().getGeneration());

        return Future.succeededFuture();
    }

    /**
     * Helper method to select the appropriate PodOperator for a cluster.
     *
     * @param clusterId The cluster ID
     * @return PodOperator for the cluster
     */
    private PodOperator selectPodOperator(String clusterId) {
        if (clusterId.equals(stretchCentralClusterId)) {
            return podOperator;
        }
        return stretchPodOperators.get(clusterId);
    }

    /**
     * Helper method to select the appropriate PodOperator for a cluster.
     *
     * @param clusterId The cluster ID
     * @return PodOperator for the cluster
     */
    private ClusterRoleBindingOperator selectClusterRoleBindingOperator(String clusterId) {
        if (clusterId.equals(stretchCentralClusterId)) {
            return clusterRoleBindingOperator;
        }
        return stretchClusterRoleBindingOperators.get(clusterId);
    }

    /**
     * Helper method to select the appropriate StrimziPodSetOperator for a cluster.
     *
     * @param clusterId The cluster ID
     * @return StrimziPodSetOperator for the cluster, or null if cluster ID is invalid
     */
    private StrimziPodSetOperator selectStrimziPodSetOperator(String clusterId) {
        if (clusterId.equals(stretchCentralClusterId)) {
            return strimziPodSetOperator;
        }
        StrimziPodSetOperator operator = stretchStrimziPodsetOperators.get(clusterId);
        if (operator == null) {
            LOGGER.warnCr(reconciliation, "Invalid cluster ID: {}. This cluster ID is not configured in STRIMZI_REMOTE_KUBE_CONFIG", clusterId);
        }
        return operator;
    }

    /**
     * Helper method to select the appropriate SecretOperator for a cluster.
     *
     * @param clusterId The cluster ID
     * @return SecretOperator for the cluster
     */
    private SecretOperator selectSecretOperator(String clusterId) {
        if (clusterId.equals(stretchCentralClusterId)) {
            return secretOperator;
        }
        return stretchSecretOperators.get(clusterId);
    }

    /**
     * Helper method to select the appropriate ConfigMapOperator for a cluster.
     *
     * @param clusterId The cluster ID
     * @return ConfigMapOperator for the cluster
     */
    private ConfigMapOperator selectConfigMapOperator(String clusterId) {
        if (clusterId.equals(stretchCentralClusterId)) {
            return configMapOperator;
        }
        return stretchConfigMapOperators.get(clusterId);
    }

    /**
     * Helper method to select the appropriate ServiceAccountOperator for a cluster.
     *
     * @param clusterId The cluster ID
     * @return ServiceAccountOperator for the cluster
     */
    private ServiceAccountOperator selectServiceAccountOperator(String clusterId) {
        if (clusterId.equals(stretchCentralClusterId)) {
            return serviceAccountOperator;
        }
        return stretchServiceAccountOperators.get(clusterId);
    }

    /**
     * Helper method to select the appropriate PvcOperator for a cluster.
     *
     * @param clusterId The cluster ID
     * @return PvcOperator for the cluster
     */
    private PvcOperator selectPvcOperator(String clusterId) {
        if (clusterId.equals(stretchCentralClusterId)) {
            return pvcOperator;
        }
        return stretchPvcOperators.get(clusterId);
    }

    /**
     * Helper method to select the appropriate StorageClassOperator for a cluster.
     *
     * @param clusterId The cluster ID
     * @return StorageClassOperator for the cluster
     */
    private StorageClassOperator selectStorageClassOperator(String clusterId) {
        if (clusterId.equals(stretchCentralClusterId)) {
            return storageClassOperator;
        }
        return stretchStorageClassOperators.get(clusterId);
    }

    /**
     * Helper method to select the appropriate ServiceOperator for a cluster.
     *
     * @param clusterId The cluster ID
     * @return ServiceOperator for the cluster
     */
    private ServiceOperator selectServiceOperator(String clusterId) {
        if (clusterId.equals(stretchCentralClusterId)) {
            return serviceOperator;
        }
        return stretchServiceOperators.get(clusterId);
    }

    /**
     * Manages certificate secrets for stretch clusters by distributing them across all clusters.
     *
     * @param clock The clock for supplying the reconciler with the time instant of each reconciliation cycle
     * @return      Future which completes when secrets are reconciled across all clusters
     */
    protected Future<Void> stretchCertificateSecrets(Clock clock) {
        List<Future<Void>> futures = new ArrayList<>();

        for (String targetClusterId : clusterIds) {
            SecretOperator secretOp = selectSecretOperator(targetClusterId);

            // Skip if operator is null (cluster not available)
            if (secretOp == null) {
                LOGGER.warnCr(reconciliation, "Skipping certificate secrets reconciliation for remote cluster '{}' - cluster not available", targetClusterId);
                continue;
            }

            boolean isCentral = targetClusterId.equals(stretchCentralClusterId);

            futures.add(
                secretOp.listAsync(reconciliation.namespace(), kafka.getSelectorLabels().withStrimziComponentType(KafkaCluster.COMPONENT_TYPE))
                    .compose(existingSecrets -> {
                        List<Secret> clusterSecrets = kafka.generateCertificatesSecrets(
                                clusterCa, 
                                clientsCa, 
                                existingSecrets,
                                listenerReconciliationResults.bootstrapDnsNames, 
                                listenerReconciliationResults.brokerDnsNames,
                                Util.isMaintenanceTimeWindowsSatisfied(
                                    reconciliation, 
                                    maintenanceWindows, 
                                    clock.instant()
                                ), 
                                targetClusterId
                            )
                            .stream()
                            .map(secret -> {
                                // For remote clusters, replace Kafka CR owner with GC ConfigMap owner
                                if (!isCentral) {
                                    String gcConfigMapName = KafkaResources.kafkaComponentName(reconciliation.name()) + "-gc";
                                    String gcUid = gcConfigMapUids.get(targetClusterId);

                                    if (gcUid == null) {
                                        LOGGER.warnCr(reconciliation, "GC ConfigMap UID not available for cluster {}, skipping owner reference for Secret {}", 
                                            targetClusterId, secret.getMetadata().getName());
                                        return secret;
                                    }

                                    return new io.fabric8.kubernetes.api.model.SecretBuilder(secret)
                                        .editMetadata()
                                            .withOwnerReferences((List<OwnerReference>) null)
                                            .addNewOwnerReference()
                                                .withApiVersion("v1")
                                                .withKind("ConfigMap")
                                                .withName(gcConfigMapName)
                                                .withUid(gcUid)
                                                .withController(false)
                                                .withBlockOwnerDeletion(false)
                                            .endOwnerReference()
                                        .endMetadata()
                                        .build();
                                }
                                return secret;
                            }).toList();

                        // Track secrets to delete for this cluster
                        List<String> desiredSecretNames = clusterSecrets.stream()
                            .map(secret -> secret.getMetadata().getName())
                            .toList();

                        existingSecrets.forEach(secret -> {
                            String secretName = secret.getMetadata().getName();
                            if (!desiredSecretNames.contains(secretName) && 
                                !KafkaResources.kafkaJmxSecretName(reconciliation.name()).equals(secretName)) {
                                stretchSecretsToDelete.computeIfAbsent(targetClusterId, k -> new ArrayList<>()).add(secretName);
                            }
                        });

                        return updateCertificateSecretsForCluster(clusterSecrets, secretOp);
                    })
            );
        }

        return Future.join(futures).mapEmpty();
    }

    /**
     * Reconciles networking resources for stretch clusters using the networking provider.
     * This creates ServiceExports and other networking resources needed for cross-cluster communication.
     * 
     * <p>The networking provider is called once per broker node per cluster. The provider is responsible
     * for creating the necessary resources (e.g., ServiceExports for MCS) and managing their ownership
     * to ensure proper garbage collection when the Kafka cluster is deleted.</p>
     *
     * @return Future which completes when networking resources are reconciled
     */
    protected Future<Void> stretchNetworkingResources() {
        if (!isStretchMode || networkingProvider == null) {
            return Future.succeededFuture();
        }

        List<Future<Void>> futures = new ArrayList<>();
        String namespace = reconciliation.namespace();

        // Build port map once - it's the same for all nodes
        Map<String, Integer> ports = buildNetworkingPorts();

        // Create networking resources for each broker in each cluster
        // Note: The plugin may deduplicate internally (e.g., MCS creates one ServiceExport per cluster)
        for (String clusterId : clusterIds) {
            for (NodeRef node : kafka.nodes()) {
                if (!node.broker()) {
                    continue; // Skip controller-only nodes
                }

                futures.add(
                    networkingProvider.createNetworkingResources(
                        reconciliation,
                        namespace,
                        node.podName(),
                        clusterId,
                        ports
                    ).mapEmpty()
                );
            }
        }

        return Future.join(futures).mapEmpty();
    }

    /**
     * Builds the port map for networking resources based on the Kafka cluster configuration.
     * This includes standard Kafka ports (replication, control plane) and user-configured listeners.
     *
     * @return Map of port names to port numbers
     */
    private Map<String, Integer> buildNetworkingPorts() {
        Map<String, Integer> ports = new HashMap<>();

        // Add standard Kafka ports (replication and control plane)
        // Port names must match those defined in KafkaCluster
        ports.put("tcp-replication", KafkaCluster.REPLICATION_PORT);
        ports.put("tcp-ctrlplane", 9090); // CONTROLPLANE_PORT is protected in KafkaCluster

        // Add user-configured listener ports
        for (GenericKafkaListener listener : kafka.getListeners()) {
            String portName = "tcp-" + listener.getName();
            ports.put(portName, listener.getPort());
        }

        return ports;
    }

    /**
     * Manages broker configuration ConfigMaps for stretch clusters by distributing them across all clusters.
     *
     * @param metricsAndLogging Metrics and logging configuration
     * @return                  Future which completes when ConfigMaps are reconciled across all clusters
     */
    protected Future<Void> stretchPerBrokerKafkaConfiguration(MetricsAndLogging metricsAndLogging) {
        List<Future<Void>> futures = new ArrayList<>();

        for (String targetClusterId : clusterIds) {
            ConfigMapOperator configMapOp = selectConfigMapOperator(targetClusterId);

            // Skip if operator is null (cluster not available)
            if (configMapOp == null) {
                LOGGER.warnCr(reconciliation, "Skipping per-broker configuration ConfigMaps reconciliation for remote cluster '{}' - cluster not available", targetClusterId);
                continue;
            }

            boolean isCentral = targetClusterId.equals(stretchCentralClusterId);

            List<ConfigMap> generatedConfigMaps = kafka.generatePerBrokerConfigurationConfigMaps(
                metricsAndLogging, 
                listenerReconciliationResults.advertisedHostnames, 
                listenerReconciliationResults.advertisedPorts, 
                targetClusterId
            );

            // Add GC ConfigMap as owner for remote cluster ConfigMaps
            final List<ConfigMap> clusterConfigMaps;
            if (!isCentral) {
                clusterConfigMaps = addGarbageCollectorOwnerReference(targetClusterId, generatedConfigMaps);
            } else {
                clusterConfigMaps = generatedConfigMaps;
            }

            LOGGER.infoOp("Desired ConfigMaps =");
            LOGGER.infoOp(clusterConfigMaps.stream().map(x -> x.getMetadata().getName()).collect(Collectors.toList()));

            futures.add(
                configMapOp.listAsync(reconciliation.namespace(), kafka.getSelectorLabels())
                    .compose(existingConfigMaps -> {

                        LOGGER.infoOp("Existing ConfigMaps =");
                        LOGGER.infoOp(existingConfigMaps.stream().map(x -> x.getMetadata().getName()).collect(Collectors.toList()));

                        List<Future<?>> ops = new ArrayList<>();

                        // Delete unwanted ConfigMaps
                        List<String> desiredNames = new ArrayList<>();
                        desiredNames.add(KafkaResources.kafkaMetricsAndLogConfigMapName(reconciliation.name()));
                        desiredNames.add(KafkaResources.kafkaComponentName(reconciliation.name()) + "-gc"); // Don't delete GC ConfigMap
                        desiredNames.addAll(clusterConfigMaps.stream().map(cm -> cm.getMetadata().getName()).toList());

                        for (ConfigMap cm : existingConfigMaps) {
                            if (!desiredNames.contains(cm.getMetadata().getName())) {
                                LOGGER.debugCr(reconciliation, "Deleting unwanted ConfigMap {} in cluster {}", 
                                        cm.getMetadata().getName(), targetClusterId);
                                ops.add(configMapOp.deleteAsync(reconciliation, reconciliation.namespace(), cm.getMetadata().getName(), true));
                            }
                        }

                        // Create/update ConfigMaps and store hashes
                        for (ConfigMap cm : clusterConfigMaps) {
                            String cmName = cm.getMetadata().getName();
                            int nodeId = ReconcilerUtils.getPodIndexFromPodName(cmName);
                            KafkaPool pool = kafka.nodePoolForNodeId(nodeId);

                            String nodeConfiguration = "";

                            if (pool.isBroker()) {
                                nodeConfiguration = listenerReconciliationResults.advertisedHostnames
                                        .get(nodeId)
                                        .entrySet()
                                        .stream()
                                        .map(kv -> kv.getKey() + "://" + kv.getValue())
                                        .sorted()
                                        .collect(Collectors.joining(" "));
                                nodeConfiguration += listenerReconciliationResults.advertisedPorts
                                        .get(nodeId)
                                        .entrySet()
                                        .stream()
                                        .map(kv -> kv.getKey() + "://" + kv.getValue())
                                        .sorted()
                                        .collect(Collectors.joining(" "));
                                nodeConfiguration += cm.getData().getOrDefault(KafkaCluster.BROKER_LISTENERS_FILENAME, "");
                            }

                            KafkaConfiguration kc = KafkaConfiguration.unvalidated(reconciliation, cm.getData().getOrDefault(KafkaCluster.BROKER_CONFIGURATION_FILENAME, ""));
                            nodeConfiguration += kc.unknownConfigsWithValues(kafka.getKafkaVersion()).toString();

                            if (pool.isController() && !pool.isBroker()) {
                                nodeConfiguration = kc.controllerConfigsWithValues().toString();
                            }

                            this.brokerConfigurationHash.put(nodeId, Util.hashStub(nodeConfiguration));

                            ops.add(configMapOp.reconcile(reconciliation, reconciliation.namespace(), cmName, cm));
                        }

                        return Future.join(ops).mapEmpty();
                    })
            );
        }

        return Future.join(futures).mapEmpty();
    }


    /**
     * Creates or updates the {@link StrimziPodSet} resources for a stretched Kafka cluster.
     *
     * If the Kafka cluster is scaled up, this method reconciles the updated PodSets and waits
     * for any new Pods to reach the Ready state. The actual startup of new Pods is handled
     * by the StrimziPodSet controller.
     *
     * Scaling down (i.e., Pod removal) is handled separately by the {@code scaleDown()} method.
     *
     * Reconciliation is performed per target cluster, and results from all clusters are merged
     * and returned as a map.
     *
     * @param podSets  List of StrimziPodSets to reconcile across all clusters.
     * @return Future that completes when reconciliation is done and all new Pods (if any) are Ready.
     */
    protected Future<Map<String, Map<String, ReconcileResult<StrimziPodSet>>>> stretchPodSet() {
        Map<String, List<StrimziPodSet>> targetedPodSets = kafka.generateClusteredPodSets(pfa.isOpenshift(), imagePullPolicy, imagePullSecrets, this::podSetPodAnnotations, stretchCentralClusterId);

        List<Future<Map<String, ReconcileResult<StrimziPodSet>>>> futures = new ArrayList<>();
        Map<String, Map<String, ReconcileResult<StrimziPodSet>>> clusteredPodSetDiff = new ConcurrentHashMap<>();

        for (String targetCluster : targetedPodSets.keySet()) {
            boolean isCentralCluster = targetCluster.equals(stretchCentralClusterId);
            StrimziPodSetOperator spsOp = selectStrimziPodSetOperator(targetCluster);

            // Skip if operator is null (cluster not available)
            if (spsOp == null) {
                LOGGER.warnCr(reconciliation, "Skipping PodSet reconciliation for remote cluster '{}' - cluster not available", targetCluster);
                continue;
            }

            // Add GC ConfigMap as owner for remote cluster resources
            List<StrimziPodSet> podSets = targetedPodSets.get(targetCluster);
            if (!isCentralCluster) {
                podSets = addGarbageCollectorOwnerReference(targetCluster, podSets);
            }

            List<StrimziPodSet> finalPodSets = podSets;
            futures.add(
                spsOp.batchReconcile(
                    reconciliation,
                    reconciliation.namespace(),
                    finalPodSets,
                    kafka.getSelectorLabels()
                ).compose(podSetDiff -> {
                    clusteredPodSetDiff
                        .computeIfAbsent(
                            targetCluster, 
                            k -> new ConcurrentHashMap<String, ReconcileResult<StrimziPodSet>>()
                        )
                        .putAll(podSetDiff);
                    return waitForNewStretchedNodes(targetCluster).map(podSetDiff);
                }));
        }

        return Future.join(futures).map(ignored -> clusteredPodSetDiff);
    }

    /**
     * Helper method to get the cluster ID for a given node.
     *
     * @param node The node reference
     * @return     The cluster ID for this node
     */
    private String getClusterIdForNode(NodeRef node) {
        // Find the pool for this node
        KafkaPool pool = kafka.nodePoolForNodeId(node.nodeId());
        if (pool == null) {
            return stretchCentralClusterId; // Default to central cluster
        }

        // Get the cluster ID from the pool's annotation
        // The pool's component name is in format "clusterName-poolName", so extract just the pool name
        String poolName = pool.getComponentName().substring(kafka.getComponentName().length() + 1);
        for (KafkaNodePool knp : kafkaNodePoolCrs) {
            if (knp.getMetadata().getName().equals(poolName)) {
                if (knp.getMetadata().getAnnotations() != null) {
                    String clusterId = knp.getMetadata().getAnnotations().get("strimzi.io/stretch-cluster-alias");
                    if (clusterId != null && !clusterId.isEmpty()) {
                        return clusterId;
                    }
                }
                break;
            }
        }

        return stretchCentralClusterId; // Default to central cluster
    }

    /**
     * The main reconciliation method which triggers the whole reconciliation pipeline. This is the method which is
     * expected to be called from the outside to trigger the reconciliation.
     *
     * @param kafkaStatus   The Kafka Status class for adding conditions to it during the reconciliation
     * @param clock         The clock for supplying the reconciler with the time instant of each reconciliation cycle.
     *                      That time is used for checking maintenance windows
     *
     * @return              Future which completes when the reconciliation completes
     */
    public Future<Void> reconcile(KafkaStatus kafkaStatus, Clock clock)    {
        return modelWarnings(kafkaStatus)
                .compose(i -> initClientAuthenticationCertificates())
                .compose(i -> manualPodCleaning())
                .compose(i -> networkPolicy())
                .compose(i -> updateKafkaAutoRebalanceStatus(kafkaStatus))
                .compose(i -> manualRollingUpdate())
                .compose(i -> pvcs(kafkaStatus))
                .compose(i -> serviceAccount())
                .compose(i -> initClusterRoleBinding())
                .compose(i -> scaleDown())
                .compose(i -> updateNodePoolStatuses(kafkaStatus))
                .compose(i -> listeners())
                .compose(i -> certificateSecrets(clock))
                .compose(i -> brokerConfigurationConfigMaps())
                .compose(i -> jmxSecret())
                .compose(i -> podDisruptionBudget())
                .compose(i -> podSet())
                .compose(podSetDiffs -> rollingUpdate(podSetDiffs)) // We pass the PodSet reconciliation result this way to avoid storing it in the instance
                .compose(i -> podsReady())
                .compose(i -> serviceEndpointsReady())
                .compose(i -> headlessServiceEndpointsReady())
                .compose(i -> clusterId(kafkaStatus))
                .compose(i -> defaultKafkaQuotas())
                .compose(i -> nodeUnregistration())
                .compose(i -> metadataVersion(kafkaStatus))
                .compose(i -> deletePersistentClaims())
                .compose(i -> sharedKafkaConfigurationCleanup())
                .compose(i -> deleteOldCertificateSecrets())
                // This has to run after all possible rolling updates which might move the pods to different nodes
                .compose(i -> nodePortExternalListenerStatus())
                .compose(i -> updateKafkaStatus(kafkaStatus));
    }

    /**
     * The stretch reconciliation method which triggers the whole stretch reconciliation pipeline.
     *
     * @param kafkaStatus   The Kafka Status class for adding conditions to it during the reconciliation
     * @param clock         The clock for supplying the reconciler with the time instant of each reconciliation cycle.
     *                      That time is used for checking maintenance windows
     *
     * @return              Future which completes when the reconciliation completes
     */
    public Future<Void> reconcileStretchedKafka(KafkaStatus kafkaStatus, Clock clock)    {
        io.strimzi.operator.cluster.stretch.StretchClusterValidator.ValidationResult configResult = 
            validator.validateKafkaConfiguration(kafkaCr, kafkaNodePoolCrs, true);

        if (!configResult.isValid()) {
            return handleValidationError(kafkaStatus, configResult);
        }

        return modelWarnings(kafkaStatus)
            .compose(i -> initClientAuthenticationCertificates())
            .compose(i -> stretchGarbageCollectorConfigMap()) // Create garbage collector ConfigMap in remote clusters FIRST
            .compose(i -> manualPodCleaning())
            .compose(i -> networkPolicy())
            .compose(i -> updateKafkaAutoRebalanceStatus(kafkaStatus))
            .compose(i -> stretchManualRollingUpdate())
            .compose(i -> stretchPvcs(kafkaStatus))
            .compose(i -> stretchServiceAccount())
            .compose(i -> stretchInitClusterRoleBinding())
            .compose(i -> stretchScaleDown())
            .compose(i -> updateNodePoolStatuses(kafkaStatus))
            .compose(i -> stretchListeners())
            .compose(i -> stretchNetworkingResources()) // Networking resources for stretch clusters
            .compose(i -> stretchCertificateSecrets(clock))
            .compose(i -> stretchBrokerConfigurationConfigMaps())
            .compose(i -> jmxSecret())
            .compose(i -> podDisruptionBudget())
            .compose(i -> stretchPodSet())
            .compose(podSetDiffs -> stretchRollingUpdate(podSetDiffs)) 
            // We pass the PodSet reconciliation result this way to avoid storing it in the instance
            .compose(i -> stretchPodsReady())
            .compose(i -> serviceEndpointsReady())
            .compose(i -> headlessServiceEndpointsReady())
            .compose(i -> clusterId(kafkaStatus))
            .compose(i -> stretchClusterStatus(kafkaStatus)) // Update stretch cluster status
            .compose(i -> defaultKafkaQuotas())
            .compose(i -> nodeUnregistration())
            .compose(i -> metadataVersion(kafkaStatus))
            .compose(i -> stretchDeletePersistentClaims())
            .compose(i -> sharedKafkaConfigurationCleanup())
            .compose(i -> stretchDeleteOldCertificateSecrets())
            // This has to run after all possible rolling updates which might move the pods to different nodes
            .compose(i -> nodePortExternalListenerStatus())
            .compose(i -> stretchListenerStatus()) // Populate listener statuses for stretch clusters
            .compose(i -> updateKafkaStatus(kafkaStatus));
    }

    /**
     * Updates the stretch cluster status in the Kafka CR status using a Condition.
     *
     * @param kafkaStatus   The Kafka Status where the stretch cluster condition will be added
     * @return              Completes when the status is updated
     */
    protected Future<Void> stretchClusterStatus(final KafkaStatus kafkaStatus) {
        if (isStretchMode && networkingProvider != null) {
            // Add a condition to indicate stretch cluster is active
            String message = String.format("Stretch cluster active: provider=%s, central=%s, clusters=%s",
                networkingProvider.getProviderName(),
                stretchCentralClusterId,
                String.join(", ", clusterIds));
    
            kafkaStatus.addCondition(new Condition() {{
                    setType("StretchCluster");
                    setStatus("True");
                    setReason("StretchClusterActive");
                    setMessage(message);
                }});
    
            LOGGER.infoCr(reconciliation, "Stretch cluster status condition added: provider={}, central={}, clusters={}", networkingProvider.getProviderName(), stretchCentralClusterId, clusterIds);
        }

        return Future.succeededFuture();
    }

    /**
     * Populates listener statuses for stretch clusters by querying Routes/Ingresses
     * for external addresses and using MCS addresses for internal listeners.
     *
     * @return Future that completes when listener statuses are populated
     */
    protected Future<Void> stretchListenerStatus() {
        if (!isStretchMode || kafkaCr.getSpec() == null || kafkaCr.getSpec().getKafka() == null 
                || kafkaCr.getSpec().getKafka().getListeners() == null) {
            return Future.succeededFuture();
        }

        List<Future<ListenerStatus>> listenerFutures = new ArrayList<>();

        for (GenericKafkaListener listener : kafkaCr.getSpec().getKafka().getListeners()) {
            listenerFutures.add(createListenerStatus(listener));
        }

        return Future.join(listenerFutures)
                .compose(result -> {
                    // Add all listener statuses to the reconciliation result
                    listenerReconciliationResults.listenerStatuses.clear();
                    for (int i = 0; i < result.size(); i++) {
                        ListenerStatus status = result.resultAt(i);
                        if (status != null) {
                            listenerReconciliationResults.listenerStatuses.add(status);
                        }
                    }
                    return Future.succeededFuture();
                });
    }

    /**
     * Creates a ListenerStatus for a single listener by querying the appropriate
     * resources (Route, Ingress, or Service) for the address.
     *
     * @param listener The listener configuration
     * @return Future with the ListenerStatus
     */
    private Future<ListenerStatus> createListenerStatus(GenericKafkaListener listener) {
        String listenerName = listener.getName();

        // Determine the listener type and get the appropriate address
        switch (listener.getType()) {
            case ROUTE:
                return getRouteAddress(listener);
            case INGRESS:
                return getIngressAddress(listener);
            case INTERNAL:
            case CLUSTER_IP:
                return getInternalAddress(listener);
            case LOADBALANCER:
            case NODEPORT:
                // These are handled by nodePortExternalListenerStatus() or similar
                // For now, return internal MCS address
                return getInternalAddress(listener);
            default:
                LOGGER.warnCr(reconciliation, "Unknown listener type {} for listener {}", 
                        listener.getType(), listenerName);
                return getInternalAddress(listener);
        }
    }

    /**
     * Gets the Route address for a route-type listener.
     * For stretch clusters, this queries routes from all clusters and returns all addresses.
     */
    private Future<ListenerStatus> getRouteAddress(GenericKafkaListener listener) {
        String bootstrapRouteName = ListenersUtils.backwardsCompatibleBootstrapRouteOrIngressName(
                reconciliation.name(), listener);
        int port = 443; // Routes use HTTPS port

        List<Future<ListenerAddress>> addressFutures = new ArrayList<>();

        // Query routes from all clusters
        for (String clusterId : clusterIds) {
            RouteOperator routeOp = selectRouteOperator(clusterId);
            addressFutures.add(
                routeOp.getAsync(reconciliation.namespace(), bootstrapRouteName)
                    .compose(route -> {
                        if (route == null || route.getStatus() == null 
                                || route.getStatus().getIngress() == null 
                                || route.getStatus().getIngress().isEmpty()) {
                            LOGGER.warnCr(reconciliation, "Route {} not ready in cluster {}", 
                                    bootstrapRouteName, clusterId);
                            return Future.succeededFuture((ListenerAddress) null);
                        }

                        String externalHost = route.getStatus().getIngress().get(0).getHost();
                        return Future.succeededFuture(new ListenerAddressBuilder()
                                .withHost(externalHost)
                                .withPort(port)
                                .build());
                    })
                    .recover(error -> {
                        LOGGER.warnCr(reconciliation, "Failed to get Route in cluster {}: {}", 
                                clusterId, error.getMessage());
                        return Future.succeededFuture((ListenerAddress) null);
                    })
            );
        }

        return Future.join(addressFutures)
                .compose(result -> {
                    List<ListenerAddress> addresses = new ArrayList<>();

                    for (int i = 0; i < result.size(); i++) {
                        ListenerAddress address = result.resultAt(i);
                        if (address != null) {
                            addresses.add(address);
                        }
                    }

                    if (addresses.isEmpty()) {
                        LOGGER.warnCr(reconciliation, "No Route addresses found, using fallback");
                        return getInternalAddress(listener);
                    }

                    ListenerStatusBuilder statusBuilder = new ListenerStatusBuilder()
                            .withName(listener.getName())
                            .withAddresses(addresses); // bootstrapServers is auto-generated from addresses

                    if (listener.isTls() && clusterCa != null) {
                        statusBuilder.withCertificates(clusterCa.currentCaCertBase64());
                    }

                    return Future.succeededFuture(statusBuilder.build());
                });
    }

    /**
     * Gets the Ingress address for an ingress-type listener.
     * For stretch clusters, this queries ingresses from all clusters and returns all addresses.
     */
    private Future<ListenerStatus> getIngressAddress(GenericKafkaListener listener) {
        String bootstrapIngressName = ListenersUtils.backwardsCompatibleBootstrapRouteOrIngressName(
                reconciliation.name(), listener);
        int port = listener.isTls() ? 443 : 80;

        List<Future<ListenerAddress>> addressFutures = new ArrayList<>();

        // Query ingresses from all clusters
        for (String clusterId : clusterIds) {
            IngressOperator ingressOp = selectIngressOperator(clusterId);
            addressFutures.add(
                ingressOp.getAsync(reconciliation.namespace(), bootstrapIngressName)
                    .compose(ingress -> {
                        if (ingress == null || ingress.getStatus() == null 
                                || ingress.getStatus().getLoadBalancer() == null 
                                || ingress.getStatus().getLoadBalancer().getIngress() == null
                                || ingress.getStatus().getLoadBalancer().getIngress().isEmpty()) {
                            LOGGER.warnCr(reconciliation, "Ingress {} not ready in cluster {}", 
                                    bootstrapIngressName, clusterId);
                            return Future.succeededFuture((ListenerAddress) null);
                        }

                        var ingressStatus = ingress.getStatus().getLoadBalancer().getIngress().get(0);
                        String externalHost = ingressStatus.getHostname() != null 
                                ? ingressStatus.getHostname() 
                                : ingressStatus.getIp();

                        return Future.succeededFuture(new ListenerAddressBuilder()
                                .withHost(externalHost)
                                .withPort(port)
                                .build());
                    })
                    .recover(error -> {
                        LOGGER.warnCr(reconciliation, "Failed to get Ingress in cluster {}: {}", 
                                clusterId, error.getMessage());
                        return Future.succeededFuture((ListenerAddress) null);
                    })
            );
        }

        return Future.join(addressFutures)
                .compose(result -> {
                    List<ListenerAddress> addresses = new ArrayList<>();

                    for (int i = 0; i < result.size(); i++) {
                        ListenerAddress address = result.resultAt(i);
                        if (address != null) {
                            addresses.add(address);
                        }
                    }

                    if (addresses.isEmpty()) {
                        LOGGER.warnCr(reconciliation, "No Ingress addresses found, using fallback");
                        return getInternalAddress(listener);
                    }

                    ListenerStatusBuilder statusBuilder = new ListenerStatusBuilder()
                            .withName(listener.getName())
                            .withAddresses(addresses); // bootstrapServers is auto-generated from addresses

                    if (listener.isTls() && clusterCa != null) {
                        statusBuilder.withCertificates(clusterCa.currentCaCertBase64());
                    }

                    return Future.succeededFuture(statusBuilder.build());
                });
    }

    /**
     * Gets the internal MCS address for internal/cluster-ip listeners.
     * For stretch clusters, this returns addresses from all clusters.
     */
    private Future<ListenerStatus> getInternalAddress(GenericKafkaListener listener) {
        String bootstrapServiceName = KafkaResources.bootstrapServiceName(kafkaCr.getMetadata().getName()) 
                + "-" + listener.getName();
        int port = listener.getPort();

        List<ListenerAddress> addresses = new ArrayList<>();

        // Add address for each cluster in the stretch setup
        for (String clusterId : clusterIds) {
            String bootstrapHost = networkingProvider != null 
                    ? networkingProvider.generateServiceDnsName(
                            reconciliation.namespace(), 
                            bootstrapServiceName, 
                            clusterId)
                    : bootstrapServiceName + "." + reconciliation.namespace() + ".svc";

            addresses.add(new ListenerAddressBuilder()
                    .withHost(bootstrapHost)
                    .withPort(port)
                    .build());
        }

        ListenerStatusBuilder statusBuilder = new ListenerStatusBuilder()
                .withName(listener.getName())
                .withAddresses(addresses); // bootstrapServers is auto-generated from addresses

        if (listener.isTls() && clusterCa != null) {
            statusBuilder.withCertificates(clusterCa.currentCaCertBase64());
        }

        return Future.succeededFuture(statusBuilder.build());
    }

    /**
     * Selects the appropriate RouteOperator for the given cluster ID.
     * Returns the central operator for the central cluster, or gets the operator from stretch operators map.
     */
    private RouteOperator selectRouteOperator(String clusterId) {
        if (clusterId.equals(stretchCentralClusterId)) {
            return routeOperator;
        }
        return stretchRouteOperators.get(clusterId);
    }

    /**
     * Selects the appropriate IngressOperator for the given cluster ID.
     * Returns the central operator for the central cluster, or gets the operator from stretch operators map.
     */
    private IngressOperator selectIngressOperator(String clusterId) {
        if (clusterId.equals(stretchCentralClusterId)) {
            return ingressOperator;
        }
        return stretchIngressOperators.get(clusterId);
    }

    private Future<Void> updateKafkaAutoRebalanceStatus(KafkaStatus kafkaStatus) {
        // gather all the desired brokers' ids across the entire cluster accounting all node pools
        Set<Integer> desiredBrokers = kafka.nodes().stream().filter(NodeRef::broker).map(NodeRef::nodeId).collect(Collectors.toSet());

        // gather all the added brokers' ids across the entire cluster accounting all node pools
        Set<Integer> addedBrokers = kafka.addedNodes().stream().filter(NodeRef::broker).map(NodeRef::nodeId).collect(Collectors.toSet());

        // if added brokers list contains all desired, it's a newly created cluster so there are no actual scaled up brokers.
        // when added brokers list has fewer nodes than desired, it actually containes the new ones for scaling up
        Set<Integer> scaledUpBrokerNodes = addedBrokers.containsAll(desiredBrokers) ? Set.of() : addedBrokers;

        KafkaRebalanceUtils.updateKafkaAutoRebalanceStatus(kafkaStatus, kafkaAutoRebalanceStatus, scaledUpBrokerNodes);

        return Future.succeededFuture();
    }

    /**
     * Takes the warning conditions from the Model and adds them in the KafkaStatus
     *
     * @param kafkaStatus   The Kafka Status where the warning conditions will be added
     *
     * @return              Completes when the warnings are added to the status object
     */
    protected Future<Void> modelWarnings(KafkaStatus kafkaStatus) {
        List<Condition> conditions = kafka.getWarningConditions();

        kafkaStatus.addConditions(conditions);

        return Future.succeededFuture();
    }

    /**
     * Initialize the TrustSet and PemAuthIdentity to be used by TLS clients during reconciliation
     *
     * @return Completes when the TrustStore and PemAuthIdentity have been created and stored in a record
     */
    protected Future<Void> initClientAuthenticationCertificates() {
        return ReconcilerUtils.coTlsPemIdentity(reconciliation, secretOperator)
                .onSuccess(coTlsPemIdentity -> this.coTlsPemIdentity = coTlsPemIdentity)
                .mapEmpty();
    }

    /**
     * Will check all Kafka pods whether the user requested the pod and PVC deletion through an annotation
     *
     * @return  Completes when the manual pod cleaning is done
     */
    protected Future<Void> manualPodCleaning() {
        return new ManualPodCleaner(
                reconciliation,
                kafka.getSelectorLabels(),
                strimziPodSetOperator,
                podOperator,
                pvcOperator
        ).maybeManualPodCleaning();
    }

    /**
     * Manages the network policy protecting the Kafka cluster
     *
     * @return  Completes when the network policy is successfully created or updated
     */
    protected Future<Void> networkPolicy() {
        if (isNetworkPolicyGeneration) {
            return networkPolicyOperator.reconcile(reconciliation, reconciliation.namespace(), KafkaResources.kafkaNetworkPolicyName(reconciliation.name()), kafka.generateNetworkPolicy(operatorNamespace, operatorNamespaceLabels))
                    .mapEmpty();
        } else {
            return Future.succeededFuture();
        }
    }

    /**
     * Does manual rolling update of Kafka pods on a stretched kafka cluster based on an annotation on the StrimziPodSet or on the Pods. Annotation
     * on StrimziPodSet level triggers rolling update of all pods. Annotation on pods triggers rolling update only of
     * the selected pods. If the annotation is present on both StrimziPodSet and one or more pods, only one rolling
     * update of all pods occurs.
     *
     * @return  Future with the result of the rolling update
     */
    protected Future<Void> stretchManualRollingUpdate() {
        List<Future<Void>> futures = new ArrayList<>();
        for (String targetClusterId : clusterIds) {
            Future<List<NodeRef>> podsToRollThroughPodSetAnno = podsForManualRollingUpdateDiscoveredThroughPodSetAnnotation(targetClusterId);
            Future<List<NodeRef>> podsToRollThroughPodAnno = podsForManualRollingUpdateDiscoveredThroughPodAnnotations(targetClusterId);

            futures.add(
                Future
                .join(podsToRollThroughPodSetAnno, podsToRollThroughPodAnno)
                .compose(result -> {
                    // We merge the lists into set to avoid duplicates
                    Set<NodeRef> nodes = new LinkedHashSet<>();
                    nodes.addAll(result.resultAt(0));
                    nodes.addAll(result.resultAt(1));

                    if (!nodes.isEmpty())   {
                        return maybeRollStretchedKafka(
                                nodes,
                                targetClusterId,
                                pod -> {
                                    if (pod == null) {
                                        throw new ConcurrentDeletionException("Unexpectedly pod no longer exists during roll of StrimziPodSet.");
                                    }

                                    LOGGER.debugCr(reconciliation, "Rolling Kafka pod {} due to manual rolling update annotation", pod.getMetadata().getName());

                                    return RestartReasons.of(RestartReason.MANUAL_ROLLING_UPDATE);
                                },
                                // Pass empty advertised hostnames and ports for the nodes
                                nodes.stream().collect(Collectors.toMap(NodeRef::nodeId, node -> Map.of())),
                                nodes.stream().collect(Collectors.toMap(NodeRef::nodeId, node -> Map.of())),
                                false
                        ).recover(error -> {
                            LOGGER.warnCr(reconciliation, "Manual rolling update failed (reconciliation will be continued)", error);
                            return Future.succeededFuture();
                        });
                    } else {
                        return Future.succeededFuture();
                    }
                })
            );
        }
        return Future.join(futures).mapEmpty();
    }

    /**
     * Checks all Kafka PodSets and if they have the manual rolling update annotation, it will take all their nodes and
     * add them to a list for rolling update.
     *
     * @return  List with node references to nodes which should be rolled
     */
    private Future<List<NodeRef>> podsForManualRollingUpdateDiscoveredThroughPodSetAnnotation()   {
        return podsForManualRollingUpdateDiscoveredThroughPodSetAnnotation(null);
    }

    /**
     * Does manual rolling update of Kafka pods based on an annotation on the StrimziPodSet or on the Pods. Annotation
     * on StrimziPodSet level triggers rolling update of all pods. Annotation on pods triggers rolling update only of
     * the selected pods. If the annotation is present on both StrimziPodSet and one or more pods, only one rolling
     * update of all pods occurs.
     *
     * @return  Future with the result of the rolling update
     */
    protected Future<Void> manualRollingUpdate() {
        Future<List<NodeRef>> podsToRollThroughPodSetAnno = podsForManualRollingUpdateDiscoveredThroughPodSetAnnotation();
        Future<List<NodeRef>> podsToRollThroughPodAnno = podsForManualRollingUpdateDiscoveredThroughPodAnnotations();

        return Future
                .join(podsToRollThroughPodSetAnno, podsToRollThroughPodAnno)
                .compose(result -> {
                    // We merge the lists into set to avoid duplicates
                    Set<NodeRef> nodes = new LinkedHashSet<>();
                    nodes.addAll(result.resultAt(0));
                    nodes.addAll(result.resultAt(1));

                    if (!nodes.isEmpty())   {
                        return maybeRollKafka(
                                nodes,
                                pod -> {
                                    if (pod == null) {
                                        throw new ConcurrentDeletionException("Unexpectedly pod no longer exists during roll of StrimziPodSet.");
                                    }

                                    LOGGER.debugCr(reconciliation, "Rolling Kafka pod {} due to manual rolling update annotation", pod.getMetadata().getName());

                                    return RestartReasons.of(RestartReason.MANUAL_ROLLING_UPDATE);
                                },
                                // Pass empty advertised hostnames and ports for the nodes
                                nodes.stream().collect(Collectors.toMap(NodeRef::nodeId, node -> Map.of())),
                                nodes.stream().collect(Collectors.toMap(NodeRef::nodeId, node -> Map.of())),
                                false
                        ).recover(error -> {
                            LOGGER.warnCr(reconciliation, "Manual rolling update failed (reconciliation will be continued)", error);
                            return Future.succeededFuture();
                        });
                    } else {
                        return Future.succeededFuture();
                    }
                });
    }

    /**
     * Checks all Kafka PodSets and if they have the manual rolling update annotation, it will take all their nodes and
     * add them to a list for rolling update.
     *
     * @return  List with node references to nodes which should be rolled
     */
    private Future<List<NodeRef>> podsForManualRollingUpdateDiscoveredThroughPodSetAnnotation(String targetClusterId)   {
        StrimziPodSetOperator operator = targetClusterId != null ? selectStrimziPodSetOperator(targetClusterId) : strimziPodSetOperator;

        if (operator == null) {
            LOGGER.warnCr(reconciliation, "Cannot list PodSets for cluster {} - operator is null (cluster not available)", targetClusterId);
            return Future.succeededFuture(List.of());
        }

        return operator.listAsync(reconciliation.namespace(), kafka.getSelectorLabels())
                .map(podSets -> {
                    List<NodeRef> nodes = new ArrayList<>();

                    for (StrimziPodSet podSet : podSets) {
                        if (Annotations.booleanAnnotation(podSet, Annotations.ANNO_STRIMZI_IO_MANUAL_ROLLING_UPDATE, false)) {
                            nodes.addAll(ReconcilerUtils.nodesFromPodSet(podSet));
                        }
                    }

                    if (!nodes.isEmpty()) {
                        LOGGER.debugCr(reconciliation, "Pods {} will be rolled due to manual rolling update annotation on their StrimziPodSet", nodes);
                    }

                    return nodes;
                });
    }

    /**
     * Checks all Kafka Pods and if they have the manual rolling update annotation, it will add them to a list for
     * rolling update.
     *
     * @return  List with node references to nodes which should be rolled
     */
    private Future<List<NodeRef>> podsForManualRollingUpdateDiscoveredThroughPodAnnotations()   {
        return podsForManualRollingUpdateDiscoveredThroughPodAnnotations(null);
    }

    /**
     * Checks all Kafka Pods and if they have the manual rolling update annotation, it will add them to a list for
     * rolling update.
     *
     * @return  List with node references to nodes which should be rolled
     */
    private Future<List<NodeRef>> podsForManualRollingUpdateDiscoveredThroughPodAnnotations(String targetClusterId)   {
        PodOperator operator = targetClusterId != null ? selectPodOperator(targetClusterId) : podOperator;

        if (operator == null) {
            LOGGER.warnCr(reconciliation, "Cannot list Pods for cluster {} - operator is null (cluster not available)", targetClusterId);
            return Future.succeededFuture(List.of());
        }

        return operator
                .listAsync(reconciliation.namespace(), kafka.getSelectorLabels())
                .map(pods -> {
                    List<NodeRef> nodes = new ArrayList<>();

                    for (Pod pod : pods) {
                        if (Annotations.booleanAnnotation(pod, Annotations.ANNO_STRIMZI_IO_MANUAL_ROLLING_UPDATE, false)) {
                            nodes.add(ReconcilerUtils.nodeFromPod(pod));
                        }
                    }

                    if (!nodes.isEmpty()) {
                        LOGGER.debugCr(reconciliation, "Pods {} will be rolled due to manual rolling update annotation on the Pod", nodes);
                    }

                    return nodes;
                });
    }

    /**
     * Rolls Kafka pods if needed
     *
     * @param nodes                     List of nodes which should be considered for rolling
     * @param podNeedsRestart           Function which serves as a predicate whether to roll pod or not
     * @param kafkaAdvertisedHostnames  Map with advertised hostnames required to generate the per-broker configuration
     * @param kafkaAdvertisedPorts      Map with advertised ports required to generate the per-broker configuration
     * @param allowReconfiguration      Defines whether the rolling update should also attempt to do dynamic reconfiguration or not
     *
     * @return  Future which completes when the rolling is complete
     */
    protected Future<Void> maybeRollKafka(
            Set<NodeRef> nodes,
            Function<Pod, RestartReasons> podNeedsRestart,
            Map<Integer, Map<String, String>> kafkaAdvertisedHostnames,
            Map<Integer, Map<String, String>> kafkaAdvertisedPorts,
            boolean allowReconfiguration
    ) {
        return new KafkaRoller(
                    reconciliation,
                    vertx,
                    podOperator,
                    1_000,
                    operationTimeoutMs,
                    () -> new BackOff(250, 2, 10),
                    nodes,
                    this.coTlsPemIdentity,
                    adminClientProvider,
                    kafkaAgentClientProvider,
                    brokerId -> kafka.generatePerBrokerConfiguration(brokerId, kafkaAdvertisedHostnames, kafkaAdvertisedPorts),
                    kafka.getKafkaVersion(),
                    allowReconfiguration,
                    eventsPublisher
            ).rollingRestart(podNeedsRestart);
    }

    /**
     * Manages the PVCs needed by the Kafka cluster. This method only creates or updates the PVCs. Deletion of PVCs
     * after scale-down happens only at the end of the reconciliation when they are not used anymore.
     *
     * @param kafkaStatus   Status of the Kafka custom resource where warnings about any issues with resizing will be added
     *
     * @return  Completes when the PVCs were successfully created or updated
     */
    protected Future<Void> pvcs(KafkaStatus kafkaStatus) {
        List<PersistentVolumeClaim> pvcs = kafka.generatePersistentVolumeClaims();

        return new PvcReconciler(reconciliation, pvcOperator, storageClassOperator)
                .resizeAndReconcilePvcs(kafkaStatus, pvcs)
                .compose(podIdsToRestart -> {
                    for (Integer podId : podIdsToRestart) {
                        try {
                            fsResizingRestartRequest.add(kafka.nodePoolForNodeId(podId).nodeRef(podId).podName());
                        } catch (KafkaCluster.NodePoolNotFoundException e) {
                            // We might have triggered some resizing on a PVC not belonging to this cluster anymore.
                            // This could happen for example with old PVCs from removed nodes. We will ignore it with
                            // a warning.
                            LOGGER.warnCr(reconciliation, "Node with ID {} does not seem to belong to this Kafka cluster and cannot be marked for restart due to storage resizing", podId);
                        }
                    }

                    return Future.succeededFuture();
                });
    }

    /**
     * Manages PVCs for stretch clusters across all target clusters.
     *
     * @param kafkaStatus   Status of the Kafka custom resource where warnings about any issues with resizing will be added
     * @return  Completes when the PVCs were successfully created or updated in all clusters
     */
    protected Future<Void> stretchPvcs(KafkaStatus kafkaStatus) {
        List<Future<Void>> futures = new ArrayList<>();

        for (String targetClusterId : clusterIds) {
            List<PersistentVolumeClaim> pvcs = kafka.generatePersistentVolumeClaimsAtCluster(targetClusterId);

            futures.add(
                new PvcReconciler(reconciliation,
                    selectPvcOperator(targetClusterId),
                    selectStorageClassOperator(targetClusterId))
                    .resizeAndReconcilePvcs(kafkaStatus, pvcs)
                    .compose(podIdsToRestart -> {
                        for (Integer podId : podIdsToRestart) {
                            try {
                                fsResizingRestartRequest.add(kafka.nodePoolForNodeId(podId).nodeRef(podId).podName());
                            } catch (KafkaCluster.NodePoolNotFoundException e) {
                                LOGGER.warnCr(reconciliation, "Node with ID {} does not seem to belong to this Kafka cluster and cannot be marked for restart due to storage resizing", podId);
                            }
                        }
                        return Future.succeededFuture();
                    })
            );
        }

        return Future.join(futures).mapEmpty();
    }

    /**
     * Manages the Kafka service account for stretch clusters across all target clusters.
     *
     * @return  Completes when the service account was successfully created or updated in all clusters
     */
    protected Future<Void> stretchServiceAccount() {
        List<Future<Void>> futures = new ArrayList<>();
        ServiceAccount serviceAccount = kafka.generateServiceAccount();

        for (String targetClusterId : clusterIds) {
            ServiceAccountOperator operator = selectServiceAccountOperator(targetClusterId);

            // Skip if operator is null (cluster not available)
            if (operator == null) {
                LOGGER.warnCr(reconciliation, "Skipping ServiceAccount reconciliation for remote cluster '{}' - cluster not available", targetClusterId);
                continue;
            }

            ServiceAccount sa = serviceAccount;

            // For remote clusters, replace Kafka CR owner reference with GC ConfigMap owner reference
            if (!targetClusterId.equals(stretchCentralClusterId)) {
                String gcConfigMapName = KafkaResources.kafkaComponentName(reconciliation.name()) + "-gc";
                String gcUid = gcConfigMapUids.get(targetClusterId);

                if (gcUid == null) {
                    LOGGER.warnCr(reconciliation, "GC ConfigMap UID not available for cluster {}, skipping ServiceAccount creation", targetClusterId);
                    continue; // Skip this cluster if UID not available
                }
                
                sa = new io.fabric8.kubernetes.api.model.ServiceAccountBuilder(serviceAccount)
                    .editMetadata()
                        .withOwnerReferences((List<OwnerReference>) null)
                        .addNewOwnerReference()
                            .withApiVersion("v1")
                            .withKind("ConfigMap")
                            .withName(gcConfigMapName)
                            .withUid(gcUid)
                            .withController(false)
                            .withBlockOwnerDeletion(false)
                        .endOwnerReference()
                    .endMetadata()
                    .build();
            }

            futures.add(
                operator.reconcile(reconciliation, reconciliation.namespace(),
                    KafkaResources.kafkaComponentName(reconciliation.name()), sa)
                    .mapEmpty()
            );
        }

        return Future.join(futures).mapEmpty();
    }

    /**
     * Creates and manages garbage collector ConfigMaps in remote clusters.
     * These ConfigMaps serve as owner references for all remote resources,
     * enabling automatic cleanup when the Kafka CR is deleted from the central cluster.
     *
     * @return Completes when the ConfigMaps are created in all remote clusters
     */
    protected Future<Void> stretchGarbageCollectorConfigMap() {
        if (!isStretchMode) {
            return Future.succeededFuture();
        }

        List<Future<Void>> futures = new ArrayList<>();

        // Only create in remote clusters (not central)
        for (String targetClusterId : clusterIds) {
            if (targetClusterId.equals(stretchCentralClusterId)) {
                continue; // Skip central cluster
            }

            ConfigMapOperator operator = selectConfigMapOperator(targetClusterId);

            // Skip if operator is null (cluster not available)
            if (operator == null) {
                LOGGER.warnCr(reconciliation, "Skipping GC ConfigMap creation for remote cluster '{}' - cluster not available", targetClusterId);
                continue;
            }

            String configMapName = KafkaResources.kafkaComponentName(reconciliation.name()) + "-gc";

            // Build the ConfigMap with metadata about managed resources
            // IMPORTANT: No owner references! This ConfigMap must NOT be garbage collected
            ConfigMap gcConfigMap = new ConfigMapBuilder()
                    .withNewMetadata()
                        .withName(configMapName)
                        .withNamespace(reconciliation.namespace())
                        .withLabels(kafka.getSelectorLabels().toMap())
                        .addToLabels("strimzi.io/kind", "Kafka")
                        .addToLabels("strimzi.io/cluster", reconciliation.name())
                        .addToLabels("strimzi.io/component-type", "garbage-collector")
                        .addToLabels("strimzi.io/remote-cluster", targetClusterId)
                        .withOwnerReferences(Collections.emptyList()) // Explicitly set empty owner references
                    .endMetadata()
                    .withData(Map.of(
                        "cluster-id", targetClusterId,
                        "kafka-cluster", reconciliation.name(),
                        "namespace", reconciliation.namespace(),
                        "purpose", "Garbage collection anchor for remote cluster resources",
                        "managed-resources", String.join(",",
                            "StrimziPodSet",
                            "ServiceAccount",
                            "Secret",
                            "ConfigMap",
                            "Service",
                            "ServiceExport",
                            "PersistentVolumeClaim"
                        )
                    ))
                    .build();

            LOGGER.infoCr(reconciliation, "Creating garbage collector ConfigMap {} in cluster {} (owner refs before reconcile: {})",
                    configMapName, targetClusterId, gcConfigMap.getMetadata().getOwnerReferences());

            futures.add(
                operator.reconcile(reconciliation, reconciliation.namespace(), configMapName, gcConfigMap)
                    .compose(result -> {
                        LOGGER.infoCr(reconciliation, "GC ConfigMap {} reconciled in cluster {}, result: {}",
                                configMapName, targetClusterId, result);
                        // Fetch the created ConfigMap to get its UID and verify owner references
                        return operator.getAsync(reconciliation.namespace(), configMapName);
                    })
                    .compose(createdConfigMap -> {
                        if (createdConfigMap != null) {
                            LOGGER.infoCr(reconciliation, "GC ConfigMap {} fetched from cluster {}, owner refs: {}",
                                    configMapName, targetClusterId, createdConfigMap.getMetadata().getOwnerReferences());
                            
                            if (createdConfigMap.getMetadata().getUid() != null) {
                                // Store the UID for later use in owner references
                                gcConfigMapUids.put(targetClusterId, createdConfigMap.getMetadata().getUid());
                                LOGGER.infoCr(reconciliation, "Garbage collector ConfigMap {} created in cluster {} with UID {}",
                                        configMapName, targetClusterId, createdConfigMap.getMetadata().getUid());
                            } else {
                                LOGGER.warnCr(reconciliation, "GC ConfigMap {} in cluster {} has no UID",
                                        configMapName, targetClusterId);
                            }
                        } else {
                            LOGGER.warnCr(reconciliation, "Could not retrieve GC ConfigMap {} from cluster {}",
                                    configMapName, targetClusterId);
                        }
                        return Future.succeededFuture();
                    }, error -> {
                        LOGGER.errorCr(reconciliation, "Failed to create/fetch GC ConfigMap {} in cluster {}: {}",
                                configMapName, targetClusterId, error.getMessage(), error);
                        return Future.succeededFuture(); // Continue with other clusters even if one fails
                    })
            );
        }

        return Future.join(futures).mapEmpty();
    }

    /**
     * Manages the Kafka service account
     *
     * @return  Completes when the service account was successfully created or updated
     */
    protected Future<Void> serviceAccount() {
        return serviceAccountOperator
                .reconcile(reconciliation, reconciliation.namespace(), KafkaResources.kafkaComponentName(reconciliation.name()), kafka.generateServiceAccount())
                .mapEmpty();
    }

    /**
     * Manages the Kafka cluster role binding in a stretchedk kafka cluster. 
     * When the desired Cluster Role Binding is null, and we get an RBAC error,
     * we ignore it. This is to allow users to run the operator only inside a namespace when no features requiring
     * Cluster Role Bindings are needed.
     *
     * @return  Completes when the Cluster Role Binding was successfully created or updated
     */
    protected Future<Void> stretchInitClusterRoleBinding() {
        ClusterRoleBinding crb = kafka.generateClusterRoleBinding(reconciliation.namespace());
        List<Future<Void>> futures = new ArrayList<>();
        for (String targetClusterId : clusterIds) {
            ClusterRoleBindingOperator crbOp = selectClusterRoleBindingOperator(targetClusterId);

            // Skip if operator is null (cluster not available)
            if (crbOp == null) {
                LOGGER.warnCr(reconciliation, "Skipping ClusterRoleBinding reconciliation for remote cluster '{}' - cluster not available", targetClusterId);
                continue;
            }
            futures.add(
                ReconcilerUtils.withIgnoreRbacError(
                    reconciliation,
                    crbOp
                        .reconcile(
                                reconciliation,
                                KafkaResources.initContainerClusterRoleBindingName(reconciliation.name(), reconciliation.namespace()),
                                crb
                        ),
                    crb
                ).mapEmpty()
            );
        }
        return Future.join(futures).mapEmpty();
    }

    /**
     * Manages the Kafka cluster role binding. When the desired Cluster Role Binding is null, and we get an RBAC error,
     * we ignore it. This is to allow users to run the operator only inside a namespace when no features requiring
     * Cluster Role Bindings are needed.
     *
     * @return  Completes when the Cluster Role Binding was successfully created or updated
     */
    protected Future<Void> initClusterRoleBinding() {
        ClusterRoleBinding desired = kafka.generateClusterRoleBinding(reconciliation.namespace());

        return ReconcilerUtils.withIgnoreRbacError(
                reconciliation,
                clusterRoleBindingOperator
                        .reconcile(
                                reconciliation,
                                KafkaResources.initContainerClusterRoleBindingName(reconciliation.name(), reconciliation.namespace()),
                                desired
                        ),
                desired
        ).mapEmpty();
    }

    /**
     * Scales down the Kafka cluster if needed. Kafka scale-down is done in one go.
     *
     * @return  Future which completes when the scale-down is finished
     */
    protected Future<Void> scaleDown() {
        return scaleDown(kafka.nodes(), strimziPodSetOperator);
    }

        /**
         * Scales down the Stretch Kafka cluster if needed. 
         *
         * @return  Future which completes when the scale-down is finished
         */
    protected Future<Void> stretchScaleDown() {

        List<Future<Void>> futures = new ArrayList<>();

        for (String targetClusterId : clusterIds) {
            StrimziPodSetOperator podSetOp = selectStrimziPodSetOperator(targetClusterId);

            // Skip if operator is null (cluster not available)
            if (podSetOp == null) {
                LOGGER.warnCr(reconciliation, "Skipping scale down for remote cluster '{}' - cluster not available", targetClusterId);
                continue;
            }

            futures.add(
                scaleDown(kafka.nodesAtCluster(targetClusterId), podSetOp)
            );
        }

        return Future.join(futures).mapEmpty();
    }

    /**
     * Scales down the Kafka cluster if needed, given the desired nodes and the
     * Strimzi Pod Set operator
     * @param desiredNodes     The end state nodes after a scale down operation
     * @param spsOp            The Strimzi Pod Set operator object which handles the
     *                         scale down operation
     *
     * @return  Future which completes when the scale-down is finished
     */
    protected Future<Void> scaleDown(Set<NodeRef> desiredNodes, StrimziPodSetOperator spsOp) {
        LOGGER.debugCr(reconciliation, "Checking if Kafka scale-down is needed");

        Set<String> desiredPodNames = new HashSet<>();
        for (NodeRef node : desiredNodes) {
            desiredPodNames.add(node.podName());
        }

        return spsOp.listAsync(reconciliation.namespace(), kafka.getSelectorLabels())
                .compose(podSets -> {
                    if (podSets == null) {
                        return Future.succeededFuture();
                    } else {
                        List<Future<Void>> ops = new ArrayList<>();

                        for (StrimziPodSet podSet : podSets) {
                            List<Map<String, Object>> desiredPods = podSet.getSpec().getPods().stream()
                                    .filter(pod -> desiredPodNames.contains(PodSetUtils.mapToPod(pod).getMetadata().getName()))
                                    .collect(Collectors.toList());

                            StrimziPodSet scaledDownPodSet = new StrimziPodSetBuilder(podSet)
                                    .editSpec()
                                    .withPods(desiredPods)
                                    .endSpec()
                                    .build();

                            if (podSet.getSpec().getPods().size() > desiredPods.size())    {
                                LOGGER.infoCr(reconciliation, "Scaling down Kafka pod set {} from {} to {} replicas", podSet.getMetadata().getName(), podSet.getSpec().getPods().size(), desiredPods.size());
                                ops.add(
                                            spsOp
                                                .reconcile(reconciliation, reconciliation.namespace(), podSet.getMetadata().getName(), scaledDownPodSet)
                                                .map((Void) null)
                                );
                            }
                        }

                        return Future.join(ops).mapEmpty();
                    }
                });
    }

    /**
     * Utility method to create the Kafka Listener reconciler. It can be also use to inject mocked reconciler during
     * tests.
     *
     * @return  KafkaListenerReconciler instance
     */
    protected KafkaListenersReconciler listenerReconciler()   {
        return new KafkaListenersReconciler(
                reconciliation,
                kafka,
                clusterCa,
                pfa,
                operationTimeoutMs,
                secretOperator,
                serviceOperator,
                routeOperator,
                ingressOperator
        );
    }

    protected Future<Void> stretchListeners() {
        return stretchListenerReconciler()
                .stretchReconcile()
                .compose(result -> {
                    listenerReconciliationResults = result;
                    return Future.succeededFuture();
                });
    }

    /**
     * Creates a StretchKafkaListenersReconciler instance for stretch clusters.
     * This reconciler uses the networking plugin to discover endpoints and generate configuration.
     *
     * @return  StretchKafkaListenersReconciler instance
     */
    protected KafkaListenersReconciler stretchListenerReconciler() {
        return new KafkaListenersReconciler(
                reconciliation,
                kafka,
                clusterCa,
                pfa,
                operationTimeoutMs,
                secretOperator,
                serviceOperator,
                routeOperator,
                ingressOperator
        ).withStretchConfig(
            clusterIds,
            stretchCentralClusterId,
            remotePfas,
            configMapOperator,
            remoteSupplier,
            networkingProvider
        );
    }

    /**
     * Reconciles listeners of this Kafka cluster
     *
     * @return  Future which completes when listeners are reconciled
     */
    protected Future<Void> listeners()    {
        return listenerReconciler()
                .reconcile()
                .compose(result -> {
                    listenerReconciliationResults = result;
                    return Future.succeededFuture();
                });
    }

    /**
     * Generates and creates the ConfigMaps with per-broker configuration for Kafka brokers used in PodSets. It will
     * also delete the ConfigMaps for any scaled-down brokers (scale down is done before this is called in the
     * reconciliation)
     *
     * @param metricsAndLogging     Metrics and Logging configuration
     *
     * @return  Future which completes when the Kafka Configuration is prepared
     */
    protected Future<Void> perBrokerKafkaConfiguration(MetricsAndLogging metricsAndLogging) {
        return configMapOperator.listAsync(reconciliation.namespace(), kafka.getSelectorLabels())
                .compose(existingConfigMaps -> {
                    List<ConfigMap> desiredConfigMaps = kafka.generatePerBrokerConfigurationConfigMaps(metricsAndLogging, listenerReconciliationResults.advertisedHostnames, listenerReconciliationResults.advertisedPorts, null);
                 
                    List<Future<?>> ops = new ArrayList<>();

                    // Delete all existing ConfigMaps which are not desired and are not the shared config map
                    List<String> desiredNames = new ArrayList<>(desiredConfigMaps.size() + 1);
                    desiredNames.add(KafkaResources.kafkaMetricsAndLogConfigMapName(reconciliation.name())); // We do not want to delete the shared ConfigMap, so we add it here
                    desiredNames.addAll(desiredConfigMaps.stream().map(cm -> cm.getMetadata().getName()).toList());

                    for (ConfigMap cm : existingConfigMaps) {
                        // We delete the cms not on the desired names list
                        if (!desiredNames.contains(cm.getMetadata().getName())) {
                            ops.add(configMapOperator.deleteAsync(reconciliation, reconciliation.namespace(), cm.getMetadata().getName(), true));
                        }
                    }

                    // Create / update the desired config maps
                    for (ConfigMap cm : desiredConfigMaps) {
                        String cmName = cm.getMetadata().getName();
                        int nodeId = ReconcilerUtils.getPodIndexFromPodName(cmName);
                        KafkaPool pool = kafka.nodePoolForNodeId(nodeId);

                        String nodeConfiguration = "";

                        // We collect the information needed for the annotation hash for brokers or mixed nodes.
                        // Controller-only nodes do not have advertised listener configuration and this config is not relevant for them.
                        if (pool.isBroker()) {
                            // The advertised hostname and port might change. If they change, we need to roll the pods.
                            // Here we collect their hash to trigger the rolling update. For per-broker configuration,
                            // we need just the advertised hostnames / ports for given broker.
                            nodeConfiguration = listenerReconciliationResults.advertisedHostnames
                                    .get(nodeId)
                                    .entrySet()
                                    .stream()
                                    .map(kv -> kv.getKey() + "://" + kv.getValue())
                                    .sorted()
                                    .collect(Collectors.joining(" "));
                            nodeConfiguration += listenerReconciliationResults.advertisedPorts
                                    .get(nodeId)
                                    .entrySet()
                                    .stream()
                                    .map(kv -> kv.getKey() + "://" + kv.getValue())
                                    .sorted()
                                    .collect(Collectors.joining(" "));
                            nodeConfiguration += cm.getData().getOrDefault(KafkaCluster.BROKER_LISTENERS_FILENAME, "");
                        }

                        // Changes to regular Kafka configuration are handled through the KafkaRoller which decides whether to roll the pod or not
                        // In addition to that, we have to handle changes to configuration unknown to Kafka -> different plugins (Authorization, Quotas etc.)
                        // This is captured here with the unknown configurations and the hash is used to roll the pod when it changes
                        KafkaConfiguration kc = KafkaConfiguration.unvalidated(reconciliation, cm.getData().getOrDefault(KafkaCluster.BROKER_CONFIGURATION_FILENAME, ""));

                        // We collect the configuration options related to various plugins
                        nodeConfiguration += kc.unknownConfigsWithValues(kafka.getKafkaVersion()).toString();

                        // We collect the information relevant to controller-only nodes
                        if (pool.isController() && !pool.isBroker())   {
                            // For controllers only, we extract the controller-relevant configurations and use it in the configuration annotations
                            nodeConfiguration = kc.controllerConfigsWithValues().toString();
                        }

                        // We store hash of the broker configurations for later use in Pod and in rolling updates
                        this.brokerConfigurationHash.put(nodeId, Util.hashStub(nodeConfiguration));

                        ops.add(configMapOperator.reconcile(reconciliation, reconciliation.namespace(), cmName, cm));
                    }

                    return Future
                            .join(ops)
                            .mapEmpty();
                });
    }

    /**
     * This method is used to create or update the config maps required by the brokers. It does not do the cleanup the
     * old shared Config Map used by StatefulSets. That is done only at the end of the reconciliation. However, it would
     * delete the config maps of the scaled-down brokers since scale-down happens before this is called.
     *
     * @return  Future which completes when the Config Map(s) with configuration are created or updated
     */
    protected Future<Void> brokerConfigurationConfigMaps() {
        return MetricsAndLoggingUtils.metricsAndLogging(reconciliation, configMapOperator, kafka.logging(), kafka.metrics())
                .compose(metricsAndLoggingCm -> perBrokerKafkaConfiguration(metricsAndLoggingCm));
    }

    protected Future<Void> stretchBrokerConfigurationConfigMaps() {
        return MetricsAndLoggingUtils.metricsAndLogging(reconciliation, configMapOperator, kafka.logging(), kafka.metrics())
            .compose(metricsAndLoggingCm -> stretchPerBrokerKafkaConfiguration(metricsAndLoggingCm));
    }

    /**
     * Manages the Secrets with the node certificates used by the Kafka nodes.
     *
     * @param clock The clock for supplying the reconciler with the time instant of each reconciliation cycle.
     *              That time is used for checking maintenance windows
     *
     * @return      Completes when the Secrets were successfully created, deleted or updated
     */
    protected Future<Void> certificateSecrets(Clock clock) {
        return secretOperator.listAsync(reconciliation.namespace(), kafka.getSelectorLabels().withStrimziComponentType(KafkaCluster.COMPONENT_TYPE))
                .compose(existingSecrets -> {
                    List<Secret> desiredCertSecrets = kafka.generateCertificatesSecrets(clusterCa, clientsCa, existingSecrets,
                            listenerReconciliationResults.bootstrapDnsNames, listenerReconciliationResults.brokerDnsNames,
                            Util.isMaintenanceTimeWindowsSatisfied(reconciliation, maintenanceWindows, clock.instant()));

                    List<String> desiredCertSecretNames = desiredCertSecrets.stream().map(secret -> secret.getMetadata().getName()).toList();
                    existingSecrets.forEach(secret -> {
                        String secretName = secret.getMetadata().getName();
                        // Don't delete desired secrets or jmx secrets
                        if (!desiredCertSecretNames.contains(secretName) && !KafkaResources.kafkaJmxSecretName(reconciliation.name()).equals(secretName)) {
                            secretsToDelete.add(secretName);
                        }
                    });
                    return updateCertificateSecrets(desiredCertSecrets);
                }).mapEmpty();
    }

    /**
     * Delete old certificate Secrets that are no longer needed.
     *
     * @return Future that completes when the Secrets have been deleted.
     */
    protected Future<Void> deleteLegacySecret() {
        List<Future<Void>> deleteFutures = new ArrayList<>();
        // Remove old Secret containing all certs if it exists
        @SuppressWarnings("deprecation")
        String oldSecretName = KafkaResources.kafkaSecretName(reconciliation.name());
        return secretOperator.getAsync(reconciliation.namespace(), oldSecretName)
                .compose(oldSecret -> {
                    if (oldSecret != null) {
                        LOGGER.debugCr(reconciliation, "Deleting legacy Secret {}/{} that is replaced by pod specific Secret.", reconciliation.namespace(), oldSecretName);
                        deleteFutures.add(secretOperator.deleteAsync(reconciliation, reconciliation.namespace(), oldSecretName, false));
                    }

                    return Future.join(deleteFutures).mapEmpty();
                });
    }

    /**
     * Delete old certificate Secrets that are no longer needed.
     *
     * @return Future that completes when the Secrets have been deleted.
     */
    protected Future<Void> deleteOldCertificateSecrets() {
        List<Future<Void>> deleteFutures = secretsToDelete.stream()
                .map(secretName -> {
                    LOGGER.debugCr(reconciliation, "Deleting old Secret {}/{} that is no longer used.", reconciliation.namespace(), secretName);
                    return secretOperator.deleteAsync(reconciliation, reconciliation.namespace(), secretName, false);
                }).collect(Collectors.toCollection(ArrayList::new)); // We need to collect to mutable list because we might need to add to the list more items later

        return Future.join(deleteFutures).compose(x -> deleteLegacySecret());
    }

    /**
     * Delete old certificate Secrets that are no longer needed.
     *
     * @return Future that completes when the Secrets have been deleted.
     */
    protected Future<Void> stretchDeleteOldCertificateSecrets() {
        List<Future<Void>> deleteFutures = new ArrayList<>();

        for (String targetClusterId : clusterIds) {
            boolean isCentralCluster = targetClusterId.equals(stretchCentralClusterId);

            List<String> secretsToDelete = stretchSecretsToDelete.get(targetClusterId);
            if (secretsToDelete != null) {
                stretchSecretsToDelete.get(targetClusterId)
                    .forEach(secretName -> {
                        LOGGER.debugCr(reconciliation, "Deleting old Secret {}/{} that is no longer used.", reconciliation.namespace(), secretName);
                        deleteFutures.add(
                            (isCentralCluster ? secretOperator : stretchSecretOperators.get(targetClusterId))
                                .deleteAsync(reconciliation, reconciliation.namespace(), secretName, false)
                        );
                    });
            }

        }

        return Future.join(deleteFutures).compose(x -> deleteLegacySecret());
    }

    /**
     * Updates the Secrets with the node certificates used by the Kafka nodes.
     *
     * @param secrets Secrets to update
     *
     * @return Future that completes when the Secrets were successfully created or updated
     */
    protected Future<Void> updateCertificateSecrets(List<Secret> secrets) {
        return updateCertificateSecretsForCluster(secrets, secretOperator);
    }

    /**
     * Updates certificate secrets for a specific cluster.
     *
     * @param secrets   List of secrets to reconcile
     * @param secretOp  SecretOperator for the target cluster
     * @return          Future which completes when secrets are updated
     */
    private Future<Void> updateCertificateSecretsForCluster(List<Secret> secrets, SecretOperator secretOp) {
        List<Future<Object>> reconcileFutures = secrets
                .stream()
                .map(secret -> {
                    String secretName = secret.getMetadata().getName();
                    return secretOp.reconcile(reconciliation, reconciliation.namespace(), secretName, secret)
                            .compose(patchResult -> {
                                if (patchResult != null) {
                                    kafkaServerCertificateHash.put(
                                            ReconcilerUtils.getPodIndexFromPodName(secretName),
                                            CertUtils.getCertificateThumbprint(patchResult.resource(),
                                                    Ca.SecretEntry.CRT.asKey(secretName)
                                            ));
                                }
                                return Future.succeededFuture();
                            });
                }).toList();
        return Future.join(reconcileFutures).mapEmpty();
    }

    /**
     * Manages the secret with JMX credentials when JMX is enabled
     *
     * @return  Completes when the JMX secret is successfully created or updated
     */
    protected Future<Void> jmxSecret() {
        return ReconcilerUtils.reconcileJmxSecret(reconciliation, secretOperator, kafka);
    }

    /**
     * Manages the PodDisruptionBudgets on Kubernetes clusters which support v1 version of PDBs
     *
     * @return  Completes when the PDB was successfully created or updated
     */
    protected Future<Void> podDisruptionBudget() {
        if (isPodDisruptionBudgetGeneration) {
            return podDisruptionBudgetOperator
                    .reconcile(reconciliation, reconciliation.namespace(), KafkaResources.kafkaComponentName(reconciliation.name()), kafka.generatePodDisruptionBudget())
                    .mapEmpty();
        } else {
            return Future.succeededFuture();
        }
    }

    /**
     * Prepares annotations for Kafka pods within a StrimziPodSet which are known only in the KafkaAssemblyOperator level.
     * These are later passed to KafkaCluster where there are used when creating the Pod definitions.
     *
     * @param node    The node for which the annotations are being prepared.
     *
     * @return  Map with Pod annotations
     */
    private Map<String, String> podSetPodAnnotations(NodeRef node) {
        Map<String, String> podAnnotations = new LinkedHashMap<>(9);
        podAnnotations.put(Ca.ANNO_STRIMZI_IO_CLUSTER_CA_CERT_GENERATION, String.valueOf(this.clusterCa.caCertGeneration()));
        podAnnotations.put(Ca.ANNO_STRIMZI_IO_CLUSTER_CA_KEY_GENERATION, String.valueOf(this.clusterCa.caKeyGeneration()));
        podAnnotations.put(Ca.ANNO_STRIMZI_IO_CLIENTS_CA_CERT_GENERATION, String.valueOf(this.clientsCa.caCertGeneration()));
        podAnnotations.put(Annotations.ANNO_STRIMZI_IO_CONFIGURATION_HASH, brokerConfigurationHash.get(node.nodeId()));
        podAnnotations.put(ANNO_STRIMZI_IO_KAFKA_VERSION, kafka.getKafkaVersion().version());
        podAnnotations.put(ANNO_STRIMZI_SERVER_CERT_HASH, kafkaServerCertificateHash.get(node.nodeId())); // Annotation of broker certificate hash

        // Annotations with custom cert thumbprints to help with rolling updates when they change
        if (node.broker() && !listenerReconciliationResults.customListenerCertificateThumbprints.isEmpty()) {
            podAnnotations.put(KafkaCluster.ANNO_STRIMZI_CUSTOM_LISTENER_CERT_THUMBPRINTS, listenerReconciliationResults.customListenerCertificateThumbprints.toString());
        }

        // If stretch clusters are enabled, add cluster id as annotation
        if (node.clusterId() != null  && !node.clusterId().isEmpty()) {

            podAnnotations.put(Annotations.ANNO_STRIMZI_STRETCH_CLUSTER_ALIAS, node.clusterId());
        }

        return podAnnotations;
    }

    /**
     * Create or update the StrimziPodSet for the Kafka cluster. If the StrimziPodSet is updated with additional pods
     * (Kafka cluster scaled up), it's the StrimziPodSet controller taking care of starting up the new nodes. But this
     * method will wait for the new nodes to get ready.
     *
     * The opposite (Kafka cluster scaled down) is handled by a dedicated scaleDown() method instead.
     *
     * @return  Future which completes when the PodSet is created, updated or deleted and any new Pods reach the Ready state
     */
    protected Future<Map<String, ReconcileResult<StrimziPodSet>>> podSet() {
        return strimziPodSetOperator
                .batchReconcile(
                        reconciliation,
                        reconciliation.namespace(),
                        kafka.generatePodSets(pfa.isOpenshift(), imagePullPolicy, imagePullSecrets, this::podSetPodAnnotations, null, null),
                        kafka.getSelectorLabels()
                )
                .compose(podSetDiff -> waitForNewNodes().map(podSetDiff));
    }

    /**
     * Waits for new nodes (pods) on a target cluster to get into a Ready state
     *
     * @return  Future that completes when all the new nodes are ready
     */
    private Future<Void> waitForNewStretchedNodes(String targetCluster) {
        boolean isCentralCluster = targetCluster.equals(stretchCentralClusterId);
        PodOperator podOp = isCentralCluster ? podOperator : stretchPodOperators.get(targetCluster);

        return ReconcilerUtils
                .podsReady(
                        reconciliation,
                        podOp,
                        operationTimeoutMs,
                        kafka.addedNodesAtCluster(targetCluster).stream().map(NodeRef::podName).toList()
                );
    }

    /**
     * Waits for new nodes (pods) to get into a Ready state
     *
     * @return  Future that completes when all the new nodes are ready
     */
    private Future<Void> waitForNewNodes() {
        return ReconcilerUtils
                .podsReady(
                        reconciliation,
                        podOperator,
                        operationTimeoutMs,
                        kafka.addedNodes().stream().map(NodeRef::podName).toList()
                );
    }

    /**
     * Rolls the Kafka broker Pods (if needed) in a stretched Kafka cluster.
     *
     * For each cluster, this method inspects the {@link ReconcileResult} of the associated
     * {@link StrimziPodSet} to determine if any Pods require a restart (e.g., due to config changes,
     * file system resizing, or certificate updates).
     *
     * Restart logic is delegated to the {@code maybeRollKafka()} method, and is performed per target cluster.
     *
     * @param podSetDiffs  Map containing the results of PodSet reconciliation across all clusters.
     * @return Future that completes when all required broker Pods (across all clusters) have been rolled.
     */
    protected Future<Void> stretchRollingUpdate(Map<String, Map<String, ReconcileResult<StrimziPodSet>>> clusteredPodSetDiff) {
        List<Future<Void>> futures = new ArrayList<>();

        for (Map.Entry<String, Map<String, ReconcileResult<StrimziPodSet>>> entry : clusteredPodSetDiff.entrySet()) {
            String targetClusterId = entry.getKey();
            Map<String, ReconcileResult<StrimziPodSet>> podSetDiff = entry.getValue();

            futures.add(maybeRollStretchedKafka(
                kafka.nodesAtCluster(targetClusterId),
                targetClusterId,
                pod -> {
                    return ReconcilerUtils.reasonsToRestartPod(
                        reconciliation,
                        podSetDiff.get(ReconcilerUtils.getControllerNameFromPodName(pod.getMetadata().getName())).resource(),
                        pod,
                        fsResizingRestartRequest,
                        ReconcilerUtils.trackedServerCertChanged(pod, kafkaServerCertificateHash),
                        clusterCa,
                        clientsCa
                    );
                },
                listenerReconciliationResults.advertisedHostnames,
                listenerReconciliationResults.advertisedPorts,
                true
            ));
        }

        return Future.join(futures).mapEmpty();
    }

    /**
     * Rolls Kafka pods in a stretched kafka cluster if needed
     *
     * @param nodes                     List of nodes which should be considered for rolling
     * @param targetClusterId           The cluster id where the rolling is supposed to happen
     * @param podNeedsRestart           Function which serves as a predicate whether to roll pod or not
     * @param kafkaAdvertisedHostnames  Map with advertised hostnames required to generate the per-broker configuration
     * @param kafkaAdvertisedPorts      Map with advertised ports required to generate the per-broker configuration
     * @param allowReconfiguration      Defines whether the rolling update should also attempt to do dynamic reconfiguration or not
     *
     * @return  Future which completes when the rolling is complete
     */
    protected Future<Void> maybeRollStretchedKafka(
            Set<NodeRef> nodes,
            String targetClusterId,
            Function<Pod, RestartReasons> podNeedsRestart,
            Map<Integer, Map<String, String>> kafkaAdvertisedHostnames,
            Map<Integer, Map<String, String>> kafkaAdvertisedPorts,
            boolean allowReconfiguration
    ) {
        return new KafkaRoller(
                    reconciliation,
                    vertx,
                    selectPodOperator(targetClusterId),
                    1_000,
                    operationTimeoutMs,
                    () -> new BackOff(250, 2, 10),
                    nodes,
                    this.coTlsPemIdentity,
                    adminClientProvider,
                    kafkaAgentClientProvider,
                    brokerId -> kafka.generatePerBrokerConfiguration(brokerId, kafkaAdvertisedHostnames, kafkaAdvertisedPorts),
                    kafka.getKafkaVersion(),
                    allowReconfiguration,
                    eventsPublisher
                )
                .withStretch(targetClusterId)
                .rollingRestart(podNeedsRestart);
    }

    /**
     * Roles the Kafka brokers (if needed).
     *
     * @param podSetDiffs   Map with the PodSet reconciliation results
     *
     * @return  Future which completes when any of the Kafka pods which need rolling is rolled
     */
    protected Future<Void> rollingUpdate(Map<String, ReconcileResult<StrimziPodSet>> podSetDiffs) {
        return maybeRollKafka(
                kafka.nodes(),
                pod -> ReconcilerUtils.reasonsToRestartPod(
                        reconciliation,
                        podSetDiffs.get(ReconcilerUtils.getControllerNameFromPodName(pod.getMetadata().getName())).resource(),
                        pod,
                        fsResizingRestartRequest,
                        ReconcilerUtils.trackedServerCertChanged(pod, kafkaServerCertificateHash),
                        clusterCa,
                        clientsCa
                ),
                listenerReconciliationResults.advertisedHostnames,
                listenerReconciliationResults.advertisedPorts,
                true
        );
    }

    /**
     * Checks whether the Kafka pods are ready and if not, waits for them to get ready
     *
     * @return  Future which completes when all Kafka pods are ready
     */
    protected Future<Void> podsReady() {
        return ReconcilerUtils
                .podsReady(
                        reconciliation,
                        podOperator,
                        operationTimeoutMs,
                        kafka.nodes().stream().map(node -> node.podName()).toList()
                );
    }

    /**
     * Checks whether the Kafka pods in a stretched kafka cluster are ready and 
     * if not, waits for them to get ready
     *
     * @return  Future which completes when all Kafka pods are ready
     */
    protected Future<Void> stretchPodsReady() {
        List<Future<Void>> futures = new ArrayList<>();
        for (String targetClusterId : clusterIds) {
            boolean isCentralCluster = targetClusterId.equals(stretchCentralClusterId);
            PodOperator podOp = isCentralCluster ? podOperator : stretchPodOperators.get(targetClusterId);
            List<NodeRef> nodes = new ArrayList<>();
            kafka.getNodePools()
                .stream()
                .filter(x -> targetClusterId.equals(x.getTargetCluster()))
                .forEach(x -> nodes.addAll(x.nodes()));

            futures.add(
                ReconcilerUtils
                    .podsReady(
                            reconciliation,
                            podOp,
                            operationTimeoutMs,
                            nodes.stream().map(node -> node.podName()).toList()
                    )
            );
        }
        return Future.join(futures).mapEmpty();
    }

    /**
     * Waits for readiness of the endpoints of the clients service
     *
     * @return  Future which completes when the endpoints are ready
     */
    protected Future<Void> serviceEndpointsReady() {
        return serviceOperator.endpointReadiness(reconciliation, reconciliation.namespace(), KafkaResources.bootstrapServiceName(reconciliation.name()), 1_000, operationTimeoutMs);
    }

    /**
     * Waits for readiness of the endpoints of the headless service
     *
     * @return  Future which completes when the endpoints are ready
     */
    protected Future<Void> headlessServiceEndpointsReady() {
        return serviceOperator.endpointReadiness(reconciliation, reconciliation.namespace(), KafkaResources.brokersServiceName(reconciliation.name()), 1_000, operationTimeoutMs);
    }

    /**
     * Get the Cluster ID of the Kafka cluster
     *
     * @return  Future which completes when the Cluster ID is retrieved and set in the status
     */
    protected Future<Void> clusterId(KafkaStatus kafkaStatus) {
        LOGGER.debugCr(reconciliation, "Attempt to get clusterId");
        return vertx.createSharedWorkerExecutor("kubernetes-ops-pool")
                .executeBlocking(() -> {
                    Admin kafkaAdmin = null;

                    try {
                        String bootstrapHostname = KafkaResources.bootstrapServiceName(reconciliation.name()) + "." + reconciliation.namespace() + ".svc:" + KafkaCluster.REPLICATION_PORT;
                        LOGGER.debugCr(reconciliation, "Creating AdminClient for clusterId using {}", bootstrapHostname);
                        kafkaAdmin = adminClientProvider.createAdminClient(bootstrapHostname, this.coTlsPemIdentity.pemTrustSet(), this.coTlsPemIdentity.pemAuthIdentity());
                        kafkaStatus.setClusterId(kafkaAdmin.describeCluster().clusterId().get());
                    } catch (KafkaException e) {
                        LOGGER.warnCr(reconciliation, "Kafka exception getting clusterId {}", e.getMessage());
                    } catch (InterruptedException e) {
                        LOGGER.warnCr(reconciliation, "Interrupted exception getting clusterId {}", e.getMessage());
                    } catch (ExecutionException e) {
                        LOGGER.warnCr(reconciliation, "Execution exception getting clusterId {}", e.getMessage());
                    } finally {
                        if (kafkaAdmin != null) {
                            kafkaAdmin.close();
                        }
                    }

                    return null;
                });
    }

    /**
     * Configures the default users quota in Kafka in case that the {@link QuotasPluginKafka} is used
     *
     * @return  Future which completes when the default quotas are configured
     */
    protected Future<Void> defaultKafkaQuotas() {
        return DefaultKafkaQuotasManager.reconcileDefaultUserQuotas(reconciliation, vertx, adminClientProvider, this.coTlsPemIdentity.pemTrustSet(), this.coTlsPemIdentity.pemAuthIdentity(), kafka.quotas());
    }

    /**
     * Unregisters the KRaft nodes that were removed from the Kafka cluster
     *
     * @return  Future which completes when the nodes removed from the Kafka cluster are unregistered
     */
    protected Future<Void> nodeUnregistration() {
        List<Integer> currentBrokerIds = kafka.brokerNodes().stream().map(NodeRef::nodeId).sorted().toList();
        Promise<Void> unregistrationPromise = Promise.promise();

        KafkaNodeUnregistration.listRegisteredBrokerNodes(reconciliation, vertx, adminClientProvider, coTlsPemIdentity.pemTrustSet(), coTlsPemIdentity.pemAuthIdentity(), true)
                .onSuccess(registeredBrokerNodes -> {

                    // all current registered broker nodes (fenced or not)
                    List<Integer> registeredBrokersIds = registeredBrokerNodes.stream()
                            .map(org.apache.kafka.common.Node::id)
                            .toList();

                    LOGGER.debugCr(reconciliation, "Nodes unregistration: currentBrokerIds = {}, registeredBrokersIds = {}",
                            currentBrokerIds, registeredBrokersIds);

                    Set<Integer> brokersIdsToUnregister = new HashSet<>(registeredBrokersIds);
                    brokersIdsToUnregister.removeAll(currentBrokerIds);

                    if (!brokersIdsToUnregister.isEmpty()) {
                        LOGGER.infoCr(reconciliation, "Kafka nodes {} were removed from the Kafka cluster and will be unregistered", brokersIdsToUnregister);

                        KafkaNodeUnregistration.unregisterBrokerNodes(reconciliation, vertx, adminClientProvider, coTlsPemIdentity.pemTrustSet(), coTlsPemIdentity.pemAuthIdentity(), brokersIdsToUnregister)
                                .onComplete(res -> {
                                    if (res.succeeded()) {
                                        LOGGER.infoCr(reconciliation, "Kafka nodes {} were successfully unregistered from the Kafka cluster", brokersIdsToUnregister);
                                    } else {
                                        // unregistration failed, we will retry on next reconciliation
                                        LOGGER.warnCr(reconciliation, "Failed to unregister Kafka nodes {} from the Kafka cluster", brokersIdsToUnregister);
                                    }

                                    // We complete the promise with success even if the unregistration failed as we do not want to
                                    // fail the reconciliation.
                                    unregistrationPromise.complete();
                                });
                    } else {
                        unregistrationPromise.complete();
                    }
                })
                .onFailure(throwable -> {
                    // listing broker nodes failed, we will retry on next reconciliation
                    LOGGER.warnCr(reconciliation, "Failed to list Kafka nodes from the Kafka cluster", throwable);
                    unregistrationPromise.complete();
                });

        return unregistrationPromise.future();
    }

    /**
     * Manages the KRaft metadata version
     *
     * @param kafkaStatus   Kafka status used for updating the currently used metadata version
     *
     * @return  Future which completes when the KRaft metadata version is set to the current version or updated.
     */
    protected Future<Void> metadataVersion(KafkaStatus kafkaStatus) {
        return KRaftMetadataManager.maybeUpdateMetadataVersion(reconciliation, vertx, this.coTlsPemIdentity, adminClientProvider, kafka.getMetadataVersion(), kafkaStatus);
    }

    /**
     * Deletion of PVCs after the Stretched Kafka Cluster is deleted is handled by owner reference and garbage collection. However,
     * this would not help after scale-downs. Therefore, we check if there are any PVCs which should not be present
     * and delete them when they are.
     *
     * This should be called only after the StrimziPodSet reconciliation, rolling update and scale-down when the PVCs
     * are not used any more by the pods.
     *
     * @return  Future which completes when the PVCs which should be deleted are deleted
     */
    protected Future<Void> stretchDeletePersistentClaims() {
        List<Future<Void>> futures = new ArrayList<>();
        for (String targetClusterId : clusterIds) {
            PvcOperator pvcOp = selectPvcOperator(targetClusterId);
            StorageClassOperator storageClassOp = selectStorageClassOperator(targetClusterId);

            // Skip if operators are null (cluster not available)
            if (pvcOp == null || storageClassOp == null) {
                LOGGER.warnCr(reconciliation, "Skipping PVC deletion for remote cluster '{}' - cluster not available", targetClusterId);
                continue;
            }
            futures.add(
                pvcOp.listAsync(reconciliation.namespace(), kafka.getSelectorLabels())
                .compose(pvcs -> {
                    List<String> maybeDeletePvcs = pvcs.stream().map(pvc -> pvc.getMetadata().getName()).collect(Collectors.toList());
                    List<String> desiredPvcs = kafka.generatePersistentVolumeClaimsAtCluster(targetClusterId).stream().map(pvc -> pvc.getMetadata().getName()).collect(Collectors.toList());

                    return new PvcReconciler(reconciliation, pvcOp, storageClassOp)
                            .deletePersistentClaims(maybeDeletePvcs, desiredPvcs);
                })
            );
        }
        return Future.join(futures).mapEmpty();
    }

    /**
     * Deletion of PVCs after the cluster is deleted is handled by owner reference and garbage collection. However,
     * this would not help after scale-downs. Therefore, we check if there are any PVCs which should not be present
     * and delete them when they are.
     *
     * This should be called only after the StrimziPodSet reconciliation, rolling update and scale-down when the PVCs
     * are not used any more by the pods.
     *
     * @return  Future which completes when the PVCs which should be deleted are deleted
     */
    protected Future<Void> deletePersistentClaims() {
        return pvcOperator.listAsync(reconciliation.namespace(), kafka.getSelectorLabels())
                .compose(pvcs -> {
                    List<String> maybeDeletePvcs = pvcs.stream().map(pvc -> pvc.getMetadata().getName()).collect(Collectors.toList());
                    List<String> desiredPvcs = kafka.generatePersistentVolumeClaims().stream().map(pvc -> pvc.getMetadata().getName()).collect(Collectors.toList());

                    return new PvcReconciler(reconciliation, pvcOperator, storageClassOperator)
                            .deletePersistentClaims(maybeDeletePvcs, desiredPvcs);
                });
    }

    /**
     * Deletes the ConfigMap with shared Kafka configuration. This needs to be done after migrating from StatefulSets to StrimziPodSets
     *
     * @return  Future which returns when the shared configuration config map is deleted
     */
    protected Future<Void> sharedKafkaConfigurationCleanup() {
        // We use reconcile() instead of deleteAsync() because reconcile first checks if the deletion is needed.
        // Deleting resource which likely does not exist would cause more load on the Kubernetes API then trying to get
        // it first because of the watch if it was deleted etc.
        return configMapOperator.reconcile(reconciliation, reconciliation.namespace(), KafkaResources.kafkaMetricsAndLogConfigMapName(reconciliation.name()), null)
                .mapEmpty();
    }

    /**
     * Creates the status for NodePort listeners. This can be done only now at the end and not when the other listener
     * types are done because it requires the Kafka brokers to be scheduled and running to collect their node addresses.
     * Without that, we do not know on which node would they be running.
     *
     * Note: To avoid issues with big clusters with many nodes, we first get the used nodes from the Pods and then get
     * the node information individually for each node instead of listing all nodes and then picking up the information
     * we need. This means more Kubernetes API calls, but helps us to avoid running out of memory.
     *
     * @return  Future which completes when the Listener status is created for all node port listeners
     */
    protected Future<Void> nodePortExternalListenerStatus() {
        if (!ListenersUtils.nodePortListeners(kafka.getListeners()).isEmpty())   {
            Map<Integer, String> brokerNodes = new HashMap<>();
            ConcurrentMap<String, Node> nodes = new ConcurrentHashMap<>();

            // First we collect all the broker pods we have so that we can find out on which worker nodes they run
            return podOperator.listAsync(reconciliation.namespace(), kafka.getSelectorLabels().withStrimziBrokerRole(true))
                    .compose(pods -> {
                        // We collect the nodes used by the brokers upfront to avoid asking for the same node multiple times later
                        for (Pod broker : pods) {
                            if (broker.getSpec() != null && broker.getSpec().getNodeName() != null) {
                                Integer podIndex = ReconcilerUtils.getPodIndexFromPodName(broker.getMetadata().getName());
                                brokerNodes.put(podIndex, broker.getSpec().getNodeName());
                            } else {
                                // This should not happen, but to avoid some chain of errors downstream we check it and raise exception
                                LOGGER.warnCr(reconciliation, "Kafka Pod {} has no node name specified", broker.getMetadata().getName());
                                return Future.failedFuture(new RuntimeException("Kafka Pod " + broker.getMetadata().getName() + " has no node name specified"));
                            }
                        }

                        List<Future<Void>> nodeFutures = new ArrayList<>();

                        // We get the full node resource for each node with a broker
                        for (String nodeName : brokerNodes.values().stream().distinct().toList()) {
                            LOGGER.debugCr(reconciliation, "Getting information on worker node {} used by one or more brokers", nodeName);
                            Future<Void> nodeFuture = nodeOperator.getAsync(nodeName).compose(node -> {
                                if (node != null) {
                                    nodes.put(nodeName, node);
                                } else {
                                    // Node was not found, but we do not want to fail because of this as it might be just some race condition
                                    LOGGER.warnCr(reconciliation, "Worker node {} does not seem to exist", nodeName);
                                }

                                return Future.succeededFuture();
                            });
                            nodeFutures.add(nodeFuture);
                        }

                        return Future.join(nodeFutures);
                    })
                    .map(i -> {
                        // We extract the address information from the nodes
                        for (GenericKafkaListener listener : ListenersUtils.nodePortListeners(kafka.getListeners())) {
                            // Set is used to ensure each node/port is listed only once. It is later converted to List.
                            Set<ListenerAddress> statusAddresses = new HashSet<>(brokerNodes.size());

                            for (Map.Entry<Integer, String> entry : brokerNodes.entrySet())   {
                                String advertisedHost = ListenersUtils.brokerAdvertisedHost(listener, kafka.nodePoolForNodeId(entry.getKey()).nodeRef(entry.getKey()));
                                ListenerAddress address;

                                if (advertisedHost != null)    {
                                    address = new ListenerAddressBuilder()
                                            .withHost(advertisedHost)
                                            .withPort(listenerReconciliationResults.bootstrapNodePorts.get(ListenersUtils.identifier(listener)))
                                            .build();
                                } else if (nodes.get(entry.getValue()) != null) {
                                    address = new ListenerAddressBuilder()
                                            .withHost(NodeUtils.findAddress(nodes.get(entry.getValue()).getStatus().getAddresses(), ListenersUtils.preferredNodeAddressType(listener)))
                                            .withPort(listenerReconciliationResults.bootstrapNodePorts.get(ListenersUtils.identifier(listener)))
                                            .build();
                                } else {
                                    // Node was not found, but we do not want to fail because of this as it might be just some race condition
                                    LOGGER.warnCr(reconciliation, "Kafka node {} is running on an unknown node and its node port address cannot be found", entry.getKey());
                                    continue;
                                }

                                statusAddresses.add(address);
                            }

                            ListenerStatus ls = listenerReconciliationResults.listenerStatuses
                                    .stream()
                                    .filter(listenerStatus -> listener.getName().equals(listenerStatus.getName()))
                                    .findFirst()
                                    .orElseThrow(() -> new RuntimeException("Status for listener " + listener.getName() + " not found"));
                            ls.setAddresses(new ArrayList<>(statusAddresses));
                        }

                        return null;
                    });
        } else {
            return Future.succeededFuture();
        }
    }

    /**
     * Updates various fields in the Kafka CR .status section such as listener information, Kafka version, metadata
     * state etc. This includes the parts of the status that do not need to be updated at a specific point in the
     * reconciliation.
     *
     * @param kafkaStatus   Kafka status where the values should be set
     *
     * @return  Future that completes once the status is updated
     */
    /* test */ Future<Void> updateKafkaStatus(KafkaStatus kafkaStatus) {
        kafkaStatus.setListeners(listenerReconciliationResults.listenerStatuses);
        kafkaStatus.setKafkaVersion(kafka.getKafkaVersion().version());
        kafkaStatus.setKafkaMetadataState(KafkaMetadataState.KRaft);

        return Future.succeededFuture();
    }

    /**
     * Updates the statuses of the used KafkaNodePools with the used node IDs. Also prepares the list of used node pools
     * for the Kafka CR status (but the Kafka status is not updated in this method).
     *
     * @param kafkaStatus   The status of the Kafka CR to add the list of node pools belonging to it
     *
     * @return  Future which completes when the statuses are set
     */
    protected Future<Void> updateNodePoolStatuses(KafkaStatus kafkaStatus) {
        List<KafkaNodePool> updatedNodePools = new ArrayList<>();
        List<UsedNodePoolStatus> statusesForKafka = new ArrayList<>();
        Map<String, KafkaNodePoolStatus> statuses = kafka.nodePoolStatuses();

        for (KafkaNodePool nodePool : kafkaNodePoolCrs) {
            statusesForKafka.add(new UsedNodePoolStatusBuilder().withName(nodePool.getMetadata().getName()).build());

            KafkaNodePool updatedNodePool = new KafkaNodePoolBuilder(nodePool)
                    .withStatus(
                            new KafkaNodePoolStatusBuilder(statuses.get(nodePool.getMetadata().getName()))
                                    .withObservedGeneration(nodePool.getMetadata().getGeneration())
                                    .build())
                    .build();

            StatusDiff diff = new StatusDiff(nodePool.getStatus(), updatedNodePool.getStatus());

            if (!diff.isEmpty()) {
                // Status changed => we will update it
                updatedNodePools.add(updatedNodePool);
            }
        }

        // Sets the list of used Node Pools in the Kafka CR status
        kafkaStatus.setKafkaNodePools(statusesForKafka.stream().sorted(Comparator.comparing(UsedNodePoolStatus::getName)).toList());

        List<Future<KafkaNodePool>> statusUpdateFutures = new ArrayList<>();

        for (KafkaNodePool updatedNodePool : updatedNodePools) {
            statusUpdateFutures.add(kafkaNodePoolOperator.updateStatusAsync(reconciliation, updatedNodePool));
        }

        // Return future
        return Future.join(statusUpdateFutures)
                .mapEmpty();
    }

    /**
     * Adds the garbage collector ConfigMap as an owner reference to a list of resources.
     * This enables automatic cleanup when the Kafka CR is deleted from the central cluster.
     *
     * @param targetClusterId The target cluster ID
     * @param resources List of resources to add owner reference to
     * @param <T> Resource type (must extend HasMetadata)
     * @return List of resources with GC ConfigMap as owner
     */
    private <T extends HasMetadata> List<T> addGarbageCollectorOwnerReference(String targetClusterId, List<T> resources) {
        String gcConfigMapName = KafkaResources.kafkaComponentName(reconciliation.name()) + "-gc";
        String gcUid = gcConfigMapUids.get(targetClusterId);
        
        if (gcUid == null) {
            LOGGER.warnCr(reconciliation, "GC ConfigMap UID not found for cluster {}, cannot add owner reference", targetClusterId);
            return resources; // Return unchanged if UID not available
        }
        
        // Create owner reference for the GC ConfigMap with the actual UID
        OwnerReference gcOwnerRef = new OwnerReferenceBuilder()
                .withApiVersion("v1")
                .withKind("ConfigMap")
                .withName(gcConfigMapName)
                .withUid(gcUid)
                .withController(false)
                .withBlockOwnerDeletion(false)
                .build();

        List<T> updatedResources = new ArrayList<>();
        for (T resource : resources) {
            // Add owner reference to the resource metadata
            if (resource.getMetadata().getOwnerReferences() == null) {
                resource.getMetadata().setOwnerReferences(new ArrayList<>());
            }
            resource.getMetadata().getOwnerReferences().add(gcOwnerRef);
            updatedResources.add(resource);
        }

        return updatedResources;
    }
}

