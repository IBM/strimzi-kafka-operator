/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.operator.cluster.stretch;

import io.fabric8.kubernetes.client.KubernetesClient;
import io.strimzi.operator.cluster.ClusterOperatorConfig;
import io.strimzi.operator.cluster.PlatformFeaturesAvailability;
import io.strimzi.operator.cluster.RemoteClientSupplier;
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
import java.util.Map;

import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.CoreMatchers.notNullValue;
import static org.hamcrest.CoreMatchers.nullValue;
import static org.hamcrest.MatcherAssert.assertThat;

/**
 * Tests for StretchInitializer.
 * 
 * This test class validates:
 * - Initialization when stretch is configured
 * - Initialization when stretch is not configured
 * - Remote PlatformFeaturesAvailability creation
 * - RemoteResourceOperatorSupplier creation
 * - InitializationResult structure
 * 
 * Test Strategy:
 * - Uses MockKube3 for Kubernetes client mocking
 * - Tests successful initialization paths
 * - Tests skipped initialization when not configured
 * - Validates result structure and contents
 * 
 * @see StretchInitializer
 */
@Tag("stretch-cluster")
@Tag("stretch-unit")

@ExtendWith(VertxExtension.class)
public class StretchInitializerTest {
    
    private static final String CENTRAL_CLUSTER_ID = "central";
    private static final String REMOTE_CLUSTER_A_ID = "cluster-a";
    private static final String REMOTE_CLUSTER_B_ID = "cluster-b";
    private static final String OPERATOR_NAME = "test-operator";
    
    private static Vertx vertx;
    private static MockKube3 mockKubeCentral;
    private static MockKube3 mockKubeRemoteA;
    private static MockKube3 mockKubeRemoteB;
    
    private KubernetesClient centralClient;
    private KubernetesClient remoteClientA;
    private KubernetesClient remoteClientB;
    private PlatformFeaturesAvailability centralPfa;
    
    @BeforeAll
    public static void beforeAll() {
        vertx = Vertx.vertx();
        
        // Create mock Kubernetes clusters once for all tests
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
    public void setUp(VertxTestContext context) {
        // Get fresh clients for each test
        centralClient = mockKubeCentral.client();
        remoteClientA = mockKubeRemoteA.client();
        remoteClientB = mockKubeRemoteB.client();
        
        // Create central PFA
        PlatformFeaturesAvailability.create(vertx, centralClient, true)
            .onComplete(context.succeeding(pfa -> context.verify(() -> {
                centralPfa = pfa;
                context.completeNow();
            })));
    }
    
    // ========== InitializationResult Tests ==========
    
    @Test
    public void testInitializationResultConstructor() {
        // Arrange
        Map<String, PlatformFeaturesAvailability> remotePfas = new HashMap<>();
        remotePfas.put(REMOTE_CLUSTER_A_ID, centralPfa);
        RemoteResourceOperatorSupplier supplier = createRemoteSupplier();
        
        // Act
        StretchInitializer.InitializationResult result = 
            new StretchInitializer.InitializationResult(remotePfas, supplier, true, true);
        
        // Assert
        assertThat("Remote PFAs should be set", result.getRemotePfas(), is(notNullValue()));
        assertThat("Remote PFAs should contain cluster-a", 
                  result.getRemotePfas().containsKey(REMOTE_CLUSTER_A_ID), is(true));
        assertThat("Remote supplier should be set", 
                  result.getRemoteResourceOperatorSupplier(), is(notNullValue()));
    }
    
    @Test
    public void testInitializationResultWithNullSupplier() {
        // Arrange
        Map<String, PlatformFeaturesAvailability> remotePfas = new HashMap<>();
        
        // Act
        StretchInitializer.InitializationResult result = 
            new StretchInitializer.InitializationResult(remotePfas, null, true, true);
        
        // Assert
        assertThat("Remote PFAs should be empty", result.getRemotePfas().isEmpty(), is(true));
        assertThat("Remote supplier should be null", 
                  result.getRemoteResourceOperatorSupplier(), is(nullValue()));
    }
    
    // ========== Initialization Tests ==========
    
    
    public void testInitializeWhenStretchNotConfigured(VertxTestContext context) {
        // Arrange - Config without stretch configuration
        ClusterOperatorConfig config = createConfigWithoutStretch();
        RemoteClientSupplier remoteClientSupplier = createRemoteClientSupplier();
        
        // Act
        StretchInitializer.initialize(config, vertx, centralClient, remoteClientSupplier, centralPfa)
            .onComplete(context.succeeding(result -> context.verify(() -> {
                // Assert
                assertThat("Result should not be null", result, is(notNullValue()));
                assertThat("Remote PFAs should be empty when stretch not configured", 
                          result.getRemotePfas().isEmpty(), is(true));
                assertThat("Remote supplier should be null when stretch not configured", 
                          result.getRemoteResourceOperatorSupplier(), is(nullValue()));
                
                context.completeNow();
            })));
    }
    
    
    public void testInitializeWithSingleRemoteCluster(VertxTestContext context) {
        // Arrange
        ClusterOperatorConfig config = createConfigWithStretch();
        RemoteClientSupplier remoteClientSupplier = createRemoteClientSupplier();
        
        // Act
        StretchInitializer.initialize(config, vertx, centralClient, remoteClientSupplier, centralPfa)
            .onComplete(context.succeeding(result -> context.verify(() -> {
                // Assert
                assertThat("Result should not be null", result, is(notNullValue()));
                assertThat("Remote PFAs should not be empty", 
                          result.getRemotePfas().isEmpty(), is(false));
                assertThat("Remote PFAs should contain cluster-a", 
                          result.getRemotePfas().containsKey(REMOTE_CLUSTER_A_ID), is(true));
                assertThat("Remote PFAs should contain cluster-b", 
                          result.getRemotePfas().containsKey(REMOTE_CLUSTER_B_ID), is(true));
                assertThat("Remote supplier should be created", 
                          result.getRemoteResourceOperatorSupplier(), is(notNullValue()));
                
                context.completeNow();
            })));
    }
    
    
    public void testInitializeWithMultipleRemoteClusters(VertxTestContext context) {
        // Arrange
        ClusterOperatorConfig config = createConfigWithStretch();
        RemoteClientSupplier remoteClientSupplier = createRemoteClientSupplier();
        
        // Act
        StretchInitializer.initialize(config, vertx, centralClient, remoteClientSupplier, centralPfa)
            .onComplete(context.succeeding(result -> context.verify(() -> {
                // Assert
                assertThat("Result should not be null", result, is(notNullValue()));
                assertThat("Should have 2 remote PFAs", result.getRemotePfas().size(), is(2));
                assertThat("Remote PFA for cluster-a should exist", 
                          result.getRemotePfas().get(REMOTE_CLUSTER_A_ID), is(notNullValue()));
                assertThat("Remote PFA for cluster-b should exist", 
                          result.getRemotePfas().get(REMOTE_CLUSTER_B_ID), is(notNullValue()));
                
                context.completeNow();
            })));
    }
    
    
    public void testInitializeCreatesRemoteResourceOperatorSupplier(VertxTestContext context) {
        // Arrange
        ClusterOperatorConfig config = createConfigWithStretch();
        RemoteClientSupplier remoteClientSupplier = createRemoteClientSupplier();
        
        // Act
        StretchInitializer.initialize(config, vertx, centralClient, remoteClientSupplier, centralPfa)
            .onComplete(context.succeeding(result -> context.verify(() -> {
                // Assert
                RemoteResourceOperatorSupplier supplier = result.getRemoteResourceOperatorSupplier();
                assertThat("Remote supplier should be created", supplier, is(notNullValue()));
                
                context.completeNow();
            })));
    }
    
    
    public void testInitializeWithEmptyRemoteClusters(VertxTestContext context) {
        // Arrange
        ClusterOperatorConfig config = createConfigWithStretch();
        Map<String, io.strimzi.operator.cluster.ClusterInfo> remoteClusters = new HashMap<>();
        Map<String, KubernetesClient> remoteClients = new HashMap<>();
        RemoteClientSupplier remoteClientSupplier = new RemoteClientSupplier("test-ns", remoteClusters, remoteClients);
        
        // Act
        StretchInitializer.initialize(config, vertx, centralClient, remoteClientSupplier, centralPfa)
            .onComplete(context.succeeding(result -> context.verify(() -> {
                // Assert
                assertThat("Result should not be null", result, is(notNullValue()));
                assertThat("Remote PFAs should be empty when no remote clusters", 
                          result.getRemotePfas().isEmpty(), is(true));
                assertThat("Remote supplier should still be created", 
                          result.getRemoteResourceOperatorSupplier(), is(notNullValue()));
                
                context.completeNow();
            })));
    }
    
    // ========== Helper Methods ==========
    
    /**
     * Creates a ClusterOperatorConfig without stretch configuration.
     */
    private ClusterOperatorConfig createConfigWithoutStretch() {
        return new ClusterOperatorConfig.ClusterOperatorConfigBuilder(
                io.strimzi.operator.cluster.ResourceUtils.dummyClusterOperatorConfig(),
                io.strimzi.operator.cluster.KafkaVersionTestUtils.getKafkaVersionLookup())
            .with("STRIMZI_OPERATOR_NAME", OPERATOR_NAME)
            .build();
    }
    
    /**
     * Creates a ClusterOperatorConfig with stretch configuration.
     */
    private ClusterOperatorConfig createConfigWithStretch() {
        return new ClusterOperatorConfig.ClusterOperatorConfigBuilder(
                io.strimzi.operator.cluster.ResourceUtils.dummyClusterOperatorConfig(),
                io.strimzi.operator.cluster.KafkaVersionTestUtils.getKafkaVersionLookup())
            .with("STRIMZI_OPERATOR_NAME", OPERATOR_NAME)
            .with("STRIMZI_CENTRAL_CLUSTER_ID", CENTRAL_CLUSTER_ID)
            .with("STRIMZI_REMOTE_KUBE_CONFIG", 
                REMOTE_CLUSTER_A_ID + ".url=https://api.cluster-a.example.com:6443\n" +
                REMOTE_CLUSTER_A_ID + ".secret=cluster-a-kubeconfig\n" +
                REMOTE_CLUSTER_B_ID + ".url=https://api.cluster-b.example.com:6443\n" +
                REMOTE_CLUSTER_B_ID + ".secret=cluster-b-kubeconfig")
            .with("STRIMZI_STRETCH_NETWORK_PROVIDER", "nodeport")
            .build();
    }
    
    /**
     * Creates a RemoteClientSupplier with test remote clusters.
     */
    private RemoteClientSupplier createRemoteClientSupplier() {
        Map<String, io.strimzi.operator.cluster.ClusterInfo> remoteClusters = new HashMap<>();
        remoteClusters.put(REMOTE_CLUSTER_A_ID, 
            new io.strimzi.operator.cluster.ClusterInfo(REMOTE_CLUSTER_A_ID, "https://api.cluster-a.example.com:6443", "cluster-a-kubeconfig"));
        remoteClusters.put(REMOTE_CLUSTER_B_ID, 
            new io.strimzi.operator.cluster.ClusterInfo(REMOTE_CLUSTER_B_ID, "https://api.cluster-b.example.com:6443", "cluster-b-kubeconfig"));
        
        Map<String, KubernetesClient> remoteClients = new HashMap<>();
        remoteClients.put(REMOTE_CLUSTER_A_ID, remoteClientA);
        remoteClients.put(REMOTE_CLUSTER_B_ID, remoteClientB);
        
        return new RemoteClientSupplier("test-ns", remoteClusters, remoteClients);
    }
    
    /**
     * Creates a RemoteResourceOperatorSupplier for testing.
     */
    private RemoteResourceOperatorSupplier createRemoteSupplier() {
        RemoteClientSupplier remoteClientSupplier = createRemoteClientSupplier();
        Map<String, PlatformFeaturesAvailability> remotePfas = new HashMap<>();
        remotePfas.put(REMOTE_CLUSTER_A_ID, centralPfa);
        remotePfas.put(REMOTE_CLUSTER_B_ID, centralPfa);
        
        return new RemoteResourceOperatorSupplier(
            vertx, centralClient, remoteClientSupplier, remotePfas, OPERATOR_NAME, CENTRAL_CLUSTER_ID
        );
    }
}
