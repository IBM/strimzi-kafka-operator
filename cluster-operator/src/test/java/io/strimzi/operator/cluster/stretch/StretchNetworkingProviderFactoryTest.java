/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.operator.cluster.stretch;

import io.fabric8.kubernetes.client.KubernetesClient;
import io.strimzi.operator.cluster.ClusterOperatorConfig;
import io.strimzi.operator.cluster.KafkaVersionTestUtils;
import io.strimzi.operator.cluster.PlatformFeaturesAvailability;
import io.strimzi.operator.cluster.RemoteClientSupplier;
import io.strimzi.operator.cluster.ResourceUtils;
import io.strimzi.operator.cluster.operator.resource.ResourceOperatorSupplier;
import io.strimzi.operator.common.InvalidConfigurationException;
import io.strimzi.platform.KubernetesVersion;
import io.strimzi.test.mockkube3.MockKube3;
import io.vertx.core.Vertx;
import io.vertx.junit5.VertxExtension;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

import java.util.HashMap;
import java.util.Map;

import static org.hamcrest.CoreMatchers.containsString;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.jupiter.api.Assertions.assertThrows;

/**
 * Tests for StretchNetworkingProviderFactory.
 * 
 * This test class validates:
 * - Plugin configuration validation
 * - Error handling for missing/invalid configuration
 * - Error messages are user-friendly and provide examples
 * 
 * Note: All providers (NodePort, LoadBalancer, MCS, etc.) are external plugins.
 * Strimzi-maintained providers (NodePort, LoadBalancer) live in a separate Strimzi repository.
 * All providers use the same loading mechanism and environment variables.
 * 
 * @see StretchNetworkingProviderFactory
 */
@Tag("stretch-cluster")
@Tag("stretch-unit")
@ExtendWith(VertxExtension.class)
public class StretchNetworkingProviderFactoryTest {
    
    private static final String CENTRAL_CLUSTER_ID = "central";
    private static final String REMOTE_CLUSTER_A_ID = "cluster-a";
    
    private static Vertx vertx;
    private static MockKube3 mockKubeCentral;
    private static MockKube3 mockKubeRemoteA;
    
    private KubernetesClient centralClient;
    private KubernetesClient remoteClientA;
    private ResourceOperatorSupplier centralSupplier;
    private RemoteResourceOperatorSupplier remoteSupplier;
    
    @BeforeAll
    public static void beforeAll() {
        vertx = Vertx.vertx();
        
        // Create mock Kubernetes clusters once for all tests
        mockKubeCentral = new MockKube3.MockKube3Builder().build();
        mockKubeCentral.start();
        
        mockKubeRemoteA = new MockKube3.MockKube3Builder().build();
        mockKubeRemoteA.start();
    }
    
    @AfterAll
    public static void afterAll() {
        if (mockKubeCentral != null) {
            mockKubeCentral.stop();
        }
        if (mockKubeRemoteA != null) {
            mockKubeRemoteA.stop();
        }
        if (vertx != null) {
            vertx.close();
        }
    }
    
    @BeforeEach
    public void setUp() {
        // Get fresh clients for each test
        centralClient = mockKubeCentral.client();
        remoteClientA = mockKubeRemoteA.client();
        
        // Create suppliers
        PlatformFeaturesAvailability pfa = new PlatformFeaturesAvailability(
            false, KubernetesVersion.MINIMAL_SUPPORTED_VERSION);
        centralSupplier = new ResourceOperatorSupplier(vertx, centralClient, 
            new io.strimzi.operator.common.MicrometerMetricsProvider(new io.micrometer.core.instrument.composite.CompositeMeterRegistry()), 
            pfa, "test-operator");
        
        Map<String, KubernetesClient> remoteClients = new HashMap<>();
        remoteClients.put(REMOTE_CLUSTER_A_ID, remoteClientA);
        
        Map<String, io.strimzi.operator.cluster.ClusterInfo> remoteClusters = new HashMap<>();
        remoteClusters.put(REMOTE_CLUSTER_A_ID, 
            new io.strimzi.operator.cluster.ClusterInfo(REMOTE_CLUSTER_A_ID, "https://api.cluster-a.example.com:6443", "cluster-a-kubeconfig"));
        
        RemoteClientSupplier remoteClientSupplier = new RemoteClientSupplier("test-ns", remoteClusters, remoteClients);
        
        Map<String, PlatformFeaturesAvailability> remotePfas = new HashMap<>();
        remotePfas.put(REMOTE_CLUSTER_A_ID, pfa);
        
        remoteSupplier = new RemoteResourceOperatorSupplier(
            vertx, centralClient, remoteClientSupplier, remotePfas, "test-operator", CENTRAL_CLUSTER_ID
        );
    }
    
    // ========== Configuration Validation Tests ==========
    
    @Test
    public void testCreateWithoutPluginClassNameFails() {

        ClusterOperatorConfig config = createOperatorConfigForCustomProvider(null, null);
        Map<String, String> providerConfig = new HashMap<>();

        // Should fail with helpful error message showing examples
        InvalidConfigurationException exception = assertThrows(InvalidConfigurationException.class, () -> {
            StretchNetworkingProviderFactory.create(
                config, providerConfig, centralSupplier, remoteSupplier
            );
        });
        
        assertThat("Exception should mention required configuration", 
                  exception.getMessage(), containsString("STRIMZI_STRETCH_PLUGIN_CLASS_NAME"));
        assertThat("Exception should provide NodePort example", 
                  exception.getMessage(), containsString("NodePortNetworkingProvider"));
        assertThat("Exception should provide LoadBalancer example", 
                  exception.getMessage(), containsString("LoadBalancerNetworkingProvider"));
        assertThat("Exception should provide MCS example", 
                  exception.getMessage(), containsString("MCSNetworkingProvider"));
    }
    
    @Test
    public void testCreateWithEmptyClassNameFails() {

        ClusterOperatorConfig config = createOperatorConfigForCustomProvider("", null);
        Map<String, String> providerConfig = new HashMap<>();
        

        // Should fail with helpful error message
        InvalidConfigurationException exception = assertThrows(InvalidConfigurationException.class, () -> {
            StretchNetworkingProviderFactory.create(
                config, providerConfig, centralSupplier, remoteSupplier
            );
        });
        
        assertThat("Exception should mention required configuration", 
                  exception.getMessage(), containsString("STRIMZI_STRETCH_PLUGIN_CLASS_NAME"));
        assertThat("Exception should provide examples", 
                  exception.getMessage(), containsString("Examples"));
    }
    
    // ========== Error Handling Tests ==========
    
    @Test
    public void testCreateWithNonExistentClassFails() {

        ClusterOperatorConfig config = createOperatorConfigForCustomProvider(
            "com.example.NonExistentProvider", null
        );
        Map<String, String> providerConfig = new HashMap<>();
        
        
        InvalidConfigurationException exception = assertThrows(InvalidConfigurationException.class, () -> {
            StretchNetworkingProviderFactory.create(
                config, providerConfig, centralSupplier, remoteSupplier
            );
        });
        
        assertThat("Exception message should mention class not found", 
                  exception.getMessage(), containsString("class not found"));
        assertThat("Exception message should mention the class name", 
                  exception.getMessage(), containsString("NonExistentProvider"));
    }
    
    // ========== Helper Methods ==========
    
    /**
     * Creates a ClusterOperatorConfig for custom provider testing.
     */
    private ClusterOperatorConfig createOperatorConfigForCustomProvider(String className, String classPath) {
        ClusterOperatorConfig.ClusterOperatorConfigBuilder builder = 
            new ClusterOperatorConfig.ClusterOperatorConfigBuilder(
                ResourceUtils.dummyClusterOperatorConfig(), 
                KafkaVersionTestUtils.getKafkaVersionLookup())
            .with("STRIMZI_CENTRAL_CLUSTER_ID", CENTRAL_CLUSTER_ID)
            .with("STRIMZI_REMOTE_KUBE_CONFIG", 
                REMOTE_CLUSTER_A_ID + ".url=https://api.cluster-a.example.com:6443\n" +
                REMOTE_CLUSTER_A_ID + ".secret=cluster-a-kubeconfig");
        
        if (className != null) {
            builder.with("STRIMZI_STRETCH_PLUGIN_CLASS_NAME", className);
        }
        if (classPath != null) {
            builder.with("STRIMZI_STRETCH_PLUGIN_CLASS_PATH", classPath);
        }
        
        return builder.build();
    }
}
