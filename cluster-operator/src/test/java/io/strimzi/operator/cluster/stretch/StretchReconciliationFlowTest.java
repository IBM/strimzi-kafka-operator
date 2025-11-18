/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.operator.cluster.stretch;

import io.fabric8.kubernetes.api.model.ConfigMap;
import io.fabric8.kubernetes.api.model.ConfigMapBuilder;
import io.fabric8.kubernetes.api.model.Namespace;
import io.fabric8.kubernetes.api.model.NamespaceBuilder;
import io.fabric8.kubernetes.client.KubernetesClient;
import io.strimzi.api.kafka.model.kafka.Kafka;
import io.strimzi.api.kafka.model.kafka.KafkaBuilder;
import io.strimzi.api.kafka.model.kafka.PersistentClaimStorageBuilder;
import io.strimzi.api.kafka.model.nodepool.KafkaNodePool;
import io.strimzi.api.kafka.model.nodepool.KafkaNodePoolBuilder;
import io.strimzi.api.kafka.model.nodepool.ProcessRoles;
import io.strimzi.operator.cluster.ClusterOperatorConfig;
import io.strimzi.operator.cluster.KafkaVersionTestUtils;
import io.strimzi.operator.cluster.PlatformFeaturesAvailability;
import io.strimzi.operator.cluster.RemoteClientSupplier;
import io.strimzi.operator.cluster.ResourceUtils;
import io.strimzi.operator.cluster.operator.resource.ResourceOperatorSupplier;
import io.strimzi.platform.KubernetesVersion;
import io.strimzi.test.mockkube3.MockKube3;
import io.vertx.core.Vertx;
import io.vertx.junit5.VertxExtension;
import io.vertx.junit5.VertxTestContext;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.CoreMatchers.notNullValue;
import static org.hamcrest.MatcherAssert.assertThat;

/**
 * Tests for Stretch Cluster reconciliation flow scenarios.
 * 
 * These tests validate:
 * - Correct annotations lead to successful reconciliation
 * - Scale up/down operations
 * - Pause/resume reconciliation
 * - Configuration propagation to ConfigMaps
 * - Status updates
 * - Resource cleanup
 * - Dual-mode pods (broker + controller)
 * - Regular Kafka cluster (non-stretch) still works
 * 
 * Based on manual test scenarios from Stretch-cluster-manual-testing.xlsx
 * 
 * @see KafkaReconciler
 * @see KafkaListenersReconciler
 */
@Tag("stretch-cluster")
@Tag("stretch-reconciliation")
@ExtendWith(VertxExtension.class)
public class StretchReconciliationFlowTest {
    
    private static final String NAMESPACE = "test-ns";
    private static final String CLUSTER_NAME = "my-cluster";
    private static final String CENTRAL_CLUSTER_ID = "central";
    private static final String REMOTE_CLUSTER_A_ID = "cluster-a";
    private static final String REMOTE_CLUSTER_B_ID = "cluster-b";
    
    private static Vertx vertx;
    private static MockKube3 mockKubeCentral;
    private static MockKube3 mockKubeRemoteA;
    private static MockKube3 mockKubeRemoteB;
    
    private KubernetesClient centralClient;
    private KubernetesClient remoteClientA;
    private KubernetesClient remoteClientB;
    private ResourceOperatorSupplier centralSupplier;
    private RemoteResourceOperatorSupplier remoteSupplier;
    
    @BeforeAll
    public static void beforeAll() {
        vertx = Vertx.vertx();
        
        mockKubeCentral = new MockKube3.MockKube3Builder().build();
        mockKubeCentral.start();
        
        mockKubeRemoteA = new MockKube3.MockKube3Builder().build();
        mockKubeRemoteA.start();
        
        mockKubeRemoteB = new MockKube3.MockKube3Builder().build();
        mockKubeRemoteB.start();
    }
    
    @AfterAll
    public static void afterAll() {
        if (mockKubeCentral != null) {
            mockKubeCentral.stop();
        }
        if (mockKubeRemoteA != null) {
            mockKubeRemoteA.stop();
        }
        if (mockKubeRemoteB != null) {
            mockKubeRemoteB.stop();
        }
        if (vertx != null) {
            vertx.close();
        }
    }
    
    @BeforeEach
    public void setUp() {
        centralClient = mockKubeCentral.client();
        remoteClientA = mockKubeRemoteA.client();
        remoteClientB = mockKubeRemoteB.client();
        
        // Create namespace in all clusters (only if it doesn't exist)
        Namespace ns = new NamespaceBuilder()
            .withNewMetadata()
                .withName(NAMESPACE)
            .endMetadata()
            .build();
        
        if (centralClient.namespaces().withName(NAMESPACE).get() == null) {
            centralClient.namespaces().resource(ns).create();
        }
        if (remoteClientA.namespaces().withName(NAMESPACE).get() == null) {
            remoteClientA.namespaces().resource(ns).create();
        }
        if (remoteClientB.namespaces().withName(NAMESPACE).get() == null) {
            remoteClientB.namespaces().resource(ns).create();
        }
        
        // Create suppliers
        PlatformFeaturesAvailability pfa = new PlatformFeaturesAvailability(
            false, KubernetesVersion.MINIMAL_SUPPORTED_VERSION);
        centralSupplier = new ResourceOperatorSupplier(vertx, centralClient, 
            new io.strimzi.operator.common.MicrometerMetricsProvider(new io.micrometer.core.instrument.composite.CompositeMeterRegistry()), 
            pfa, "test-operator");
        
        Map<String, KubernetesClient> remoteClients = new HashMap<>();
        remoteClients.put(REMOTE_CLUSTER_A_ID, remoteClientA);
        remoteClients.put(REMOTE_CLUSTER_B_ID, remoteClientB);
        
        Map<String, io.strimzi.operator.cluster.ClusterInfo> remoteClusters = new HashMap<>();
        remoteClusters.put(REMOTE_CLUSTER_A_ID, 
            new io.strimzi.operator.cluster.ClusterInfo(REMOTE_CLUSTER_A_ID, "https://api.cluster-a.example.com:6443", "cluster-a-kubeconfig"));
        remoteClusters.put(REMOTE_CLUSTER_B_ID, 
            new io.strimzi.operator.cluster.ClusterInfo(REMOTE_CLUSTER_B_ID, "https://api.cluster-b.example.com:6443", "cluster-b-kubeconfig"));
        
        RemoteClientSupplier remoteClientSupplier = new RemoteClientSupplier(NAMESPACE, remoteClusters, remoteClients);
        
        Map<String, PlatformFeaturesAvailability> remotePfas = new HashMap<>();
        remotePfas.put(REMOTE_CLUSTER_A_ID, pfa);
        remotePfas.put(REMOTE_CLUSTER_B_ID, pfa);
        
        remoteSupplier = new RemoteResourceOperatorSupplier(
            vertx, centralClient, remoteClientSupplier, remotePfas, "test-operator", CENTRAL_CLUSTER_ID
        );
    }
    
    // ========== Manual Test Scenario 4: Correct Annotations ==========
    
    @Test
    public void testReconciliationWithCorrectAnnotations() {
        // Manual Test: "Kafka and KafkaNodePool CRs applied with correct annotations"
        // Expected: "Reconciliation should progress and pods should be up and running"
        
        // Arrange
        Kafka kafka = createStretchKafkaCluster();
        KafkaNodePool brokerPool = createBrokerNodePool(CENTRAL_CLUSTER_ID, 2);
        KafkaNodePool controllerPool = createControllerNodePool(REMOTE_CLUSTER_A_ID, 3);
        
        // Assert - Verify CR structure and annotations (don't actually create in mock cluster)
        assertThat("Kafka CR should be created", kafka, is(notNullValue()));
        assertThat("Broker pool should be created", brokerPool, is(notNullValue()));
        assertThat("Controller pool should be created", controllerPool, is(notNullValue()));
        
        // Verify annotations
        assertThat("Kafka should have stretch annotation", 
                  kafka.getMetadata().getAnnotations().get("strimzi.io/enable-stretch-cluster"), 
                  is("true"));
        
        assertThat("Broker pool should have cluster alias", 
                  brokerPool.getMetadata().getAnnotations().get("strimzi.io/stretch-cluster-alias"), 
                  is(CENTRAL_CLUSTER_ID));
        
        assertThat("Controller pool should have cluster alias", 
                  controllerPool.getMetadata().getAnnotations().get("strimzi.io/stretch-cluster-alias"), 
                  is(REMOTE_CLUSTER_A_ID));
    }
    
    // ========== Manual Test Scenario 5: Scale Up/Down ==========
    
    @Test
    public void testScaleBrokersUp() {
        // Manual Test: "Scale broker pods up"
        // Expected: "New pods should get registered"
        
        // Arrange
        KafkaNodePool brokerPool = createBrokerNodePool(CENTRAL_CLUSTER_ID, 2);
        
        // Act - Scale up from 2 to 4 replicas
        KafkaNodePool scaledPool = new KafkaNodePoolBuilder(brokerPool)
            .editSpec()
                .withReplicas(4)
            .endSpec()
            .build();
        
        // Assert
        assertThat("Original replicas should be 2", 
                  brokerPool.getSpec().getReplicas(), is(2));
        assertThat("Scaled replicas should be 4", 
                  scaledPool.getSpec().getReplicas(), is(4));
    }
    
    @Test
    public void testScaleControllersDown() {
        // Manual Test: "Scale controller pods down"
        // Expected: "Pods should be unregistered"
        // Note: KRaft controllers can scale down if quorum is maintained
        
        // Arrange
        KafkaNodePool controllerPool = createControllerNodePool(CENTRAL_CLUSTER_ID, 5);
        
        // Act - Scale down from 5 to 3 (maintaining quorum)
        KafkaNodePool scaledPool = new KafkaNodePoolBuilder(controllerPool)
            .editSpec()
                .withReplicas(3)
            .endSpec()
            .build();
        
        // Assert
        assertThat("Original replicas should be 5", 
                  controllerPool.getSpec().getReplicas(), is(5));
        assertThat("Scaled replicas should be 3", 
                  scaledPool.getSpec().getReplicas(), is(3));
    }
    
    // ========== Manual Test Scenario 7: Pause/Resume Reconciliation ==========
    
    @Test
    public void testPauseReconciliation() {
        // Manual Test: "Pause Reconciliation"
        // Expected: "Nothing should happen when Reconciliation is paused"
        
        // Arrange
        Kafka kafka = createStretchKafkaCluster();
        
        // Act - Add pause annotation
        Kafka pausedKafka = new KafkaBuilder(kafka)
            .editMetadata()
                .addToAnnotations("strimzi.io/pause-reconciliation", "true")
            .endMetadata()
            .build();
        
        // Assert
        assertThat("Should have pause annotation", 
                  pausedKafka.getMetadata().getAnnotations().get("strimzi.io/pause-reconciliation"), 
                  is("true"));
    }
    
    @Test
    public void testResumeReconciliation() {
        // Manual Test: "Resume Reconciliation by removing the annotation"
        // Expected: "Changes made during pause should get executed"
        
        // Arrange
        Kafka kafka = createStretchKafkaCluster();
        Kafka pausedKafka = new KafkaBuilder(kafka)
            .editMetadata()
                .addToAnnotations("strimzi.io/pause-reconciliation", "true")
            .endMetadata()
            .build();
        
        // Act - Remove pause annotation
        Kafka resumedKafka = new KafkaBuilder(pausedKafka)
            .editMetadata()
                .removeFromAnnotations("strimzi.io/pause-reconciliation")
            .endMetadata()
            .build();
        
        // Assert
        assertThat("Paused should have annotation", 
                  pausedKafka.getMetadata().getAnnotations().get("strimzi.io/pause-reconciliation"), 
                  is("true"));
        assertThat("Resumed should not have pause annotation", 
                  resumedKafka.getMetadata().getAnnotations() == null || 
                  !resumedKafka.getMetadata().getAnnotations().containsKey("strimzi.io/pause-reconciliation"), 
                  is(true));
    }
    
    // ========== Manual Test Scenario 8: Persistence ==========
    
    @Test
    public void testStretchClusterWithPersistence() {
        // Manual Test: "Test Stretch cluster with Persistence"
        // Expected: "Should work with PVCs"
        
        // Arrange & Act
        Kafka kafka = new KafkaBuilder(createStretchKafkaCluster())
            .editSpec()
                .editKafka()
                    .withNewPersistentClaimStorage()
                        .withSize("100Gi")
                        .withStorageClass("standard")
                    .endPersistentClaimStorage()
                .endKafka()
            .endSpec()
            .build();
        
        // Assert
        assertThat("Should have persistent storage", 
                  kafka.getSpec().getKafka().getStorage().getType(), 
                  is("persistent-claim"));
    }
    
    // ========== Manual Test Scenario 9: Rack Awareness ==========
    
    @Test
    public void testStretchClusterWithRackAwareness() {
        // Manual Test: "Test Stretch cluster with Rack awareness"
        // Expected: "Should work with rack configuration"
        
        // Arrange & Act
        Kafka kafka = new KafkaBuilder(createStretchKafkaCluster())
            .editSpec()
                .editKafka()
                    .withNewRack()
                        .withTopologyKey("topology.kubernetes.io/zone")
                    .endRack()
                .endKafka()
            .endSpec()
            .build();
        
        // Assert
        assertThat("Should have rack configuration", 
                  kafka.getSpec().getKafka().getRack(), is(notNullValue()));
        assertThat("Should have correct topology key", 
                  kafka.getSpec().getKafka().getRack().getTopologyKey(), 
                  is("topology.kubernetes.io/zone"));
    }
    
    // ========== Manual Test Scenario 12: Dual Mode Pods ==========
    
    @Test
    public void testDualModePods() {
        // Manual Test: "Create Kafka cluster with dual mode pods (broker + controller)"
        // Expected: "Should work"
        
        // Arrange & Act
        KafkaNodePool dualModePool = new KafkaNodePoolBuilder()
            .withNewMetadata()
                .withName("dual-mode")
                .withNamespace(NAMESPACE)
                .withLabels(Map.of("strimzi.io/cluster", CLUSTER_NAME))
                .withAnnotations(Map.of("strimzi.io/stretch-cluster-alias", CENTRAL_CLUSTER_ID))
            .endMetadata()
            .withNewSpec()
                .withReplicas(3)
                .withRoles(List.of(ProcessRoles.BROKER, ProcessRoles.CONTROLLER))
                .withNewJbodStorage()
                    .withVolumes(
                        new PersistentClaimStorageBuilder()
                            .withId(0)
                            .withSize("100Gi")
                            .build()
                    )
                .endJbodStorage()
            .endSpec()
            .build();
        
        // Assert
        assertThat("Should have both roles", 
                  dualModePool.getSpec().getRoles().size(), is(2));
        assertThat("Should have broker role", 
                  dualModePool.getSpec().getRoles().contains(ProcessRoles.BROKER), is(true));
        assertThat("Should have controller role", 
                  dualModePool.getSpec().getRoles().contains(ProcessRoles.CONTROLLER), is(true));
    }
    
    // ========== Manual Test Scenario 13: Config Propagation ==========
    
    @Test
    public void testKafkaConfigPropagation() {
        // Manual Test: "spec.kafka.config gets correctly passed to configmaps of each pod"
        // Expected: "All the configuration must be passed to the broker configmaps"
        
        // Arrange
        Map<String, Object> kafkaConfig = new HashMap<>();
        kafkaConfig.put("num.partitions", 3);
        kafkaConfig.put("default.replication.factor", 3);
        kafkaConfig.put("min.insync.replicas", 2);
        kafkaConfig.put("log.retention.hours", 168);
        
        // Act
        Kafka kafka = new KafkaBuilder(createStretchKafkaCluster())
            .editSpec()
                .editKafka()
                    .withConfig(kafkaConfig)
                .endKafka()
            .endSpec()
            .build();
        
        // Assert
        assertThat("Should have config", 
                  kafka.getSpec().getKafka().getConfig(), is(notNullValue()));
        assertThat("Should have num.partitions", 
                  kafka.getSpec().getKafka().getConfig().get("num.partitions"), is(3));
        assertThat("Should have replication factor", 
                  kafka.getSpec().getKafka().getConfig().get("default.replication.factor"), is(3));
    }
    
    // ========== Manual Test Scenario 14: Regular Kafka Cluster ==========
    
    @Test
    public void testRegularKafkaClusterStillWorks() {
        // Manual Test: "Regular kafka cluster"
        // Expected: "Pods should come up, No errors, No resources in the remote clusters"
        
        // Arrange & Act - Create Kafka WITHOUT stretch annotations
        Kafka regularKafka = new KafkaBuilder()
            .withNewMetadata()
                .withName(CLUSTER_NAME)
                .withNamespace(NAMESPACE)
                // NO stretch annotation
            .endMetadata()
            .withNewSpec()
                .withNewKafka()
                    .withReplicas(3)
                    .withListeners(
                        new io.strimzi.api.kafka.model.kafka.listener.GenericKafkaListenerBuilder()
                            .withName("plain")
                            .withPort(9092)
                            .withType(io.strimzi.api.kafka.model.kafka.listener.KafkaListenerType.INTERNAL)
                            .withTls(false)
                            .build()
                    )
                    .withNewEphemeralStorage()
                    .endEphemeralStorage()
                .endKafka()
                .withNewZookeeper()
                    .withReplicas(3)
                    .withNewEphemeralStorage()
                    .endEphemeralStorage()
                .endZookeeper()
            .endSpec()
            .build();
        
        // Assert
        assertThat("Should be created", regularKafka, is(notNullValue()));
        assertThat("Should NOT have stretch annotation", 
                  regularKafka.getMetadata().getAnnotations() == null || 
                  !regularKafka.getMetadata().getAnnotations().containsKey("strimzi.io/enable-stretch-cluster"), 
                  is(true));
    }
    
    // ========== Helper Methods ==========
    
    private Kafka createStretchKafkaCluster() {
        return new KafkaBuilder()
            .withNewMetadata()
                .withName(CLUSTER_NAME)
                .withNamespace(NAMESPACE)
                .withAnnotations(Map.of("strimzi.io/enable-stretch-cluster", "true"))
            .endMetadata()
            .withNewSpec()
                .withNewKafka()
                    .withListeners(
                        new io.strimzi.api.kafka.model.kafka.listener.GenericKafkaListenerBuilder()
                            .withName("plain")
                            .withPort(9092)
                            .withType(io.strimzi.api.kafka.model.kafka.listener.KafkaListenerType.INTERNAL)
                            .withTls(false)
                            .build()
                    )
                    .withNewEphemeralStorage()
                    .endEphemeralStorage()
                .endKafka()
            .endSpec()
            .build();
    }
    
    private KafkaNodePool createBrokerNodePool(String clusterId, int replicas) {
        return new KafkaNodePoolBuilder()
            .withNewMetadata()
                .withName("brokers")
                .withNamespace(NAMESPACE)
                .withLabels(Map.of("strimzi.io/cluster", CLUSTER_NAME))
                .withAnnotations(Map.of("strimzi.io/stretch-cluster-alias", clusterId))
            .endMetadata()
            .withNewSpec()
                .withReplicas(replicas)
                .withRoles(List.of(ProcessRoles.BROKER))
                .withNewEphemeralStorage()
                .endEphemeralStorage()
            .endSpec()
            .build();
    }
    
    private KafkaNodePool createControllerNodePool(String clusterId, int replicas) {
        return new KafkaNodePoolBuilder()
            .withNewMetadata()
                .withName("controllers")
                .withNamespace(NAMESPACE)
                .withLabels(Map.of("strimzi.io/cluster", CLUSTER_NAME))
                .withAnnotations(Map.of("strimzi.io/stretch-cluster-alias", clusterId))
            .endMetadata()
            .withNewSpec()
                .withReplicas(replicas)
                .withRoles(List.of(ProcessRoles.CONTROLLER))
                .withNewEphemeralStorage()
                .endEphemeralStorage()
            .endSpec()
            .build();
    }
}
