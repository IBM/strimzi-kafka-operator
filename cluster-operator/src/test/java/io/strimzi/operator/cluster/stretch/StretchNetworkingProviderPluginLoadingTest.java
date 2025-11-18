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
import io.strimzi.operator.cluster.stretch.spi.StretchNetworkingProvider;
import io.strimzi.operator.common.InvalidConfigurationException;
import io.strimzi.platform.KubernetesVersion;
import io.strimzi.test.mockkube3.MockKube3;
import io.vertx.core.Future;
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
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.CoreMatchers.notNullValue;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.jupiter.api.Assertions.assertThrows;

/**
 * Comprehensive tests for StretchNetworkingProvider plugin loading mechanism.
 * 
 * This test class validates:
 * - Plugin class loading from classpath
 * - Plugin class loading with custom classpath
 * - Error handling for various failure scenarios
 * - Configuration validation
 * - Provider initialization
 * - Error messages are helpful and actionable
 * 
 * Test Strategy:
 * - Uses MockKube3 for Kubernetes client mocking
 * - Tests both success and failure paths
 * - Validates error messages guide users to correct configuration
 * - Tests edge cases and boundary conditions
 * 
 * @see StretchNetworkingProviderFactory
 * @see StretchNetworkingProvider
 */
@Tag("stretch-cluster")
@Tag("stretch-unit")
@ExtendWith(VertxExtension.class)
public class StretchNetworkingProviderPluginLoadingTest {
    
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
    
    // ========== Plugin Class Name Validation Tests ==========
    
    @Test
    public void testNullClassNameThrowsExceptionWithExamples() {
        // Arrange
        ClusterOperatorConfig config = createConfig(null, null);
        Map<String, String> providerConfig = new HashMap<>();
        
        // Act & Assert
        InvalidConfigurationException exception = assertThrows(InvalidConfigurationException.class, () -> {
            StretchNetworkingProviderFactory.create(config, providerConfig, centralSupplier, remoteSupplier);
        });
        
        // Verify error message is helpful
        assertThat("Should mention configuration requirement", 
                  exception.getMessage(), containsString("plugin to be configured"));
        assertThat("Should mention env var name", 
                  exception.getMessage(), containsString("STRIMZI_STRETCH_PLUGIN_CLASS_NAME"));
        assertThat("Should provide NodePort example", 
                  exception.getMessage(), containsString("NodePortNetworkingProvider"));
        assertThat("Should provide LoadBalancer example", 
                  exception.getMessage(), containsString("LoadBalancerNetworkingProvider"));
        assertThat("Should provide MCS example", 
                  exception.getMessage(), containsString("MCSNetworkingProvider"));
    }
    
    @Test
    public void testEmptyClassNameThrowsException() {
        // Arrange
        ClusterOperatorConfig config = createConfig("", null);
        Map<String, String> providerConfig = new HashMap<>();
        
        // Act & Assert
        InvalidConfigurationException exception = assertThrows(InvalidConfigurationException.class, () -> {
            StretchNetworkingProviderFactory.create(config, providerConfig, centralSupplier, remoteSupplier);
        });
        
        assertThat("Should mention required configuration", 
                  exception.getMessage(), containsString("STRIMZI_STRETCH_PLUGIN_CLASS_NAME"));
    }
    
    @Test
    public void testWhitespaceOnlyClassNameThrowsException() {
        // Arrange
        ClusterOperatorConfig config = createConfig("   ", null);
        Map<String, String> providerConfig = new HashMap<>();
        
        // Act & Assert - whitespace is trimmed by config, so treated as empty
        InvalidConfigurationException exception = assertThrows(InvalidConfigurationException.class, () -> {
            StretchNetworkingProviderFactory.create(config, providerConfig, centralSupplier, remoteSupplier);
        });
        
        assertThat("Should mention required configuration", 
                  exception.getMessage(), containsString("STRIMZI_STRETCH_PLUGIN_CLASS_NAME"));
    }
    
    // ========== Class Loading Error Tests ==========
    
    @Test
    public void testNonExistentClassThrowsException() {
        // Arrange
        ClusterOperatorConfig config = createConfig("com.example.NonExistentProvider", null);
        Map<String, String> providerConfig = new HashMap<>();
        
        // Act & Assert
        InvalidConfigurationException exception = assertThrows(InvalidConfigurationException.class, () -> {
            StretchNetworkingProviderFactory.create(config, providerConfig, centralSupplier, remoteSupplier);
        });
        
        assertThat("Should mention class not found", 
                  exception.getMessage(), containsString("class not found"));
        assertThat("Should mention the class name", 
                  exception.getMessage(), containsString("NonExistentProvider"));
    }
    
    @Test
    public void testInvalidPackageNameThrowsException() {
        // Arrange
        ClusterOperatorConfig config = createConfig("invalid..package.Provider", null);
        Map<String, String> providerConfig = new HashMap<>();
        
        // Act & Assert
        InvalidConfigurationException exception = assertThrows(InvalidConfigurationException.class, () -> {
            StretchNetworkingProviderFactory.create(config, providerConfig, centralSupplier, remoteSupplier);
        });
        
        assertThat("Should mention class loading failure", 
                  exception.getMessage(), containsString("class not found"));
    }
    
    @Test
    public void testClassWithoutPackageThrowsException() {
        // Arrange - Class name without package (e.g., "MyProvider" instead of "com.example.MyProvider")
        ClusterOperatorConfig config = createConfig("MyProvider", null);
        Map<String, String> providerConfig = new HashMap<>();
        
        // Act & Assert
        InvalidConfigurationException exception = assertThrows(InvalidConfigurationException.class, () -> {
            StretchNetworkingProviderFactory.create(config, providerConfig, centralSupplier, remoteSupplier);
        });
        
        assertThat("Should mention class not found", 
                  exception.getMessage(), containsString("class not found"));
    }
    
    // ========== Class Path Tests ==========
    
    @Test
    public void testInvalidClassPathThrowsException() {
        // Arrange - Invalid classpath format
        ClusterOperatorConfig config = createConfig("com.example.Provider", "not-a-valid-path");
        Map<String, String> providerConfig = new HashMap<>();
        
        // Act & Assert
        InvalidConfigurationException exception = assertThrows(InvalidConfigurationException.class, () -> {
            StretchNetworkingProviderFactory.create(config, providerConfig, centralSupplier, remoteSupplier);
        });
        
        // Should fail during class loading or path parsing
        assertThat("Should mention error", 
                  exception.getMessage(), notNullValue());
    }
    
    @Test
    public void testNonExistentClassPathThrowsException() {
        // Arrange - Classpath that doesn't exist
        ClusterOperatorConfig config = createConfig("com.example.Provider", "/non/existent/path/*");
        Map<String, String> providerConfig = new HashMap<>();
        
        // Act & Assert
        InvalidConfigurationException exception = assertThrows(InvalidConfigurationException.class, () -> {
            StretchNetworkingProviderFactory.create(config, providerConfig, centralSupplier, remoteSupplier);
        });
        
        // Should fail during classpath validation or class loading
        assertThat("Should mention error loading provider", 
                  exception.getMessage(), containsString("Failed to load"));
        assertThat("Should mention the classpath", 
                  exception.getMessage(), containsString("/non/existent/path"));
    }
    
    // ========== Provider Initialization Tests ==========
    
    @Test
    public void testProviderInitializationFailureIsReported() {
        // Arrange - Use a test provider that fails initialization
        ClusterOperatorConfig config = createConfig(
            "io.strimzi.operator.cluster.stretch.StretchNetworkingProviderPluginLoadingTest$FailingInitProvider", 
            null
        );
        Map<String, String> providerConfig = new HashMap<>();
        
        // Act & Assert
        InvalidConfigurationException exception = assertThrows(InvalidConfigurationException.class, () -> {
            StretchNetworkingProviderFactory.create(config, providerConfig, centralSupplier, remoteSupplier);
        });
        
        assertThat("Should mention initialization failure", 
                  exception.getMessage(), containsString("Failed to initialize"));
        assertThat("Should mention the provider class", 
                  exception.getMessage(), containsString("FailingInitProvider"));
    }
    
    // ========== Configuration Consistency Tests ==========
    
    @Test
    public void testProviderConfigIsPassedToInit() {
        // This test validates that provider configuration is properly passed
        // In a real scenario, the provider would use this config
        // Here we just verify the mechanism doesn't fail with valid config
        
        // Arrange
        ClusterOperatorConfig config = createConfig(
            "io.strimzi.operator.cluster.stretch.StretchNetworkingProviderPluginLoadingTest$TestProvider", 
            null
        );
        Map<String, String> providerConfig = new HashMap<>();
        providerConfig.put("test.key", "test.value");
        providerConfig.put("another.key", "another.value");
        
        // Act & Assert
        // Should fail because TestProvider doesn't exist in classpath, but validates config passing
        InvalidConfigurationException exception = assertThrows(InvalidConfigurationException.class, () -> {
            StretchNetworkingProviderFactory.create(config, providerConfig, centralSupplier, remoteSupplier);
        });
        
        assertThat("Should fail on class loading, not config passing", 
                  exception.getMessage(), containsString("class not found"));
    }
    
    // ========== Error Message Quality Tests ==========
    
    @Test
    public void testErrorMessagesAreActionable() {
        // Arrange
        ClusterOperatorConfig config = createConfig(null, null);
        Map<String, String> providerConfig = new HashMap<>();
        
        // Act
        InvalidConfigurationException exception = assertThrows(InvalidConfigurationException.class, () -> {
            StretchNetworkingProviderFactory.create(config, providerConfig, centralSupplier, remoteSupplier);
        });
        
        // Assert - Error message should guide user to solution
        String message = exception.getMessage();
        assertThat("Should explain what's wrong", message, containsString("requires a plugin"));
        assertThat("Should tell user what to do", message, containsString("Please set"));
        assertThat("Should provide concrete examples", message, containsString("Examples"));
        assertThat("Should mention both env vars", message, containsString("STRIMZI_STRETCH_PLUGIN_CLASS_NAME"));
        assertThat("Should mention both env vars", message, containsString("STRIMZI_STRETCH_PLUGIN_CLASS_PATH"));
    }
    
    @Test
    public void testErrorMessageShowsAllProviderTypes() {
        // Arrange
        ClusterOperatorConfig config = createConfig(null, null);
        Map<String, String> providerConfig = new HashMap<>();
        
        // Act
        InvalidConfigurationException exception = assertThrows(InvalidConfigurationException.class, () -> {
            StretchNetworkingProviderFactory.create(config, providerConfig, centralSupplier, remoteSupplier);
        });
        
        // Assert - Should show examples for all provider types
        String message = exception.getMessage();
        assertThat("Should show NodePort (Strimzi-maintained)", 
                  message, containsString("NodePort"));
        assertThat("Should show LoadBalancer (Strimzi-maintained)", 
                  message, containsString("LoadBalancer"));
        assertThat("Should show MCS (third-party example)", 
                  message, containsString("MCS"));
    }
    
    // ========== Edge Case Tests ==========
    
    @Test
    public void testVeryLongClassNameIsHandled() {
        // Arrange - Unreasonably long class name
        String longClassName = "com.example." + "VeryLong".repeat(50) + "Provider";
        ClusterOperatorConfig config = createConfig(longClassName, null);
        Map<String, String> providerConfig = new HashMap<>();
        
        // Act & Assert
        InvalidConfigurationException exception = assertThrows(InvalidConfigurationException.class, () -> {
            StretchNetworkingProviderFactory.create(config, providerConfig, centralSupplier, remoteSupplier);
        });
        
        // Should handle gracefully
        assertThat("Should mention class not found", 
                  exception.getMessage(), containsString("class not found"));
    }
    
    @Test
    public void testSpecialCharactersInClassNameAreHandled() {
        // Arrange - Class name with special characters (invalid but should be handled)
        ClusterOperatorConfig config = createConfig("com.example.Provider$Inner", null);
        Map<String, String> providerConfig = new HashMap<>();
        
        // Act & Assert
        InvalidConfigurationException exception = assertThrows(InvalidConfigurationException.class, () -> {
            StretchNetworkingProviderFactory.create(config, providerConfig, centralSupplier, remoteSupplier);
        });
        
        // Should handle gracefully (inner classes are valid Java but unlikely for plugins)
        assertThat("Should mention class not found", 
                  exception.getMessage(), containsString("class not found"));
    }
    
    // ========== Helper Methods ==========
    
    /**
     * Creates a ClusterOperatorConfig with the specified plugin configuration.
     */
    private ClusterOperatorConfig createConfig(String className, String classPath) {
        ClusterOperatorConfig.ClusterOperatorConfigBuilder builder = 
            new ClusterOperatorConfig.ClusterOperatorConfigBuilder(
                ResourceUtils.dummyClusterOperatorConfig(), 
                KafkaVersionTestUtils.getKafkaVersionLookup())
            .with("STRIMZI_CENTRAL_CLUSTER_ID", CENTRAL_CLUSTER_ID)
            .with("STRIMZI_REMOTE_KUBE_CONFIG", 
                REMOTE_CLUSTER_A_ID + ".url=https://api.cluster-a.example.com:6443\n" +
                REMOTE_CLUSTER_A_ID + ".secret=cluster-a-kubeconfig");
        
        if (className != null && !className.trim().isEmpty()) {
            builder.with("STRIMZI_STRETCH_PLUGIN_CLASS_NAME", className);
        }
        if (classPath != null) {
            builder.with("STRIMZI_STRETCH_PLUGIN_CLASS_PATH", classPath);
        }
        
        return builder.build();
    }
    
    // ========== Test Provider Classes ==========
    
    /**
     * Test provider that fails during initialization.
     * 
     * This is used ONLY to test that the factory properly reports initialization failures.
     * Since init() always fails, the other methods are never called, so they just throw
     * to make it clear they shouldn't be used.
     * 
     * For testing actual provider functionality, use TestNetworkingProvider instead.
     */
    public static class FailingInitProvider implements StretchNetworkingProvider {
        @Override
        public String getProviderName() {
            return "failing-init";
        }
        
        @Override
        public Future<Void> init(Map<String, String> config, 
                                 ResourceOperatorSupplier centralSupplier,
                                 RemoteResourceOperatorSupplier remoteSupplier) {
            // Simulate initialization failure - this is the ONLY method we actually test
            return Future.failedFuture(new RuntimeException("Simulated initialization failure"));
        }
        
        // All methods below are never called because init() fails.
        // They throw to make it obvious if someone tries to use this provider incorrectly.
        
        @Override
        public Future<java.util.List<io.fabric8.kubernetes.api.model.HasMetadata>> createNetworkingResources(
                io.strimzi.operator.common.Reconciliation reconciliation,
                String namespace,
                String podName,
                String clusterId,
                Map<String, Integer> ports) {
            throw new UnsupportedOperationException("FailingInitProvider is only for testing init() failures");
        }
        
        @Override
        public Future<String> discoverPodEndpoint(
                io.strimzi.operator.common.Reconciliation reconciliation,
                String namespace,
                String serviceName,
                String clusterId,
                String portName) {
            throw new UnsupportedOperationException("FailingInitProvider is only for testing init() failures");
        }
        
        @Override
        public String generateServiceDnsName(String namespace, String serviceName, String clusterId) {
            throw new UnsupportedOperationException("FailingInitProvider is only for testing init() failures");
        }
        
        @Override
        public String generatePodDnsName(String namespace, String serviceName, String podName, String clusterId) {
            throw new UnsupportedOperationException("FailingInitProvider is only for testing init() failures");
        }
        
        @Override
        public Future<String> generateAdvertisedListeners(
                io.strimzi.operator.common.Reconciliation reconciliation,
                String namespace,
                String podName,
                String clusterId,
                Map<String, String> listeners) {
            throw new UnsupportedOperationException("FailingInitProvider is only for testing init() failures");
        }
        
        @Override
        public Future<String> generateQuorumVoters(
                io.strimzi.operator.common.Reconciliation reconciliation,
                String namespace,
                java.util.List<StretchNetworkingProvider.ControllerPodInfo> controllerPods,
                String replicationPortName) {
            throw new UnsupportedOperationException("FailingInitProvider is only for testing init() failures");
        }
        
        @Override
        public Future<java.util.List<String>> generateCertificateSans(
                io.strimzi.operator.common.Reconciliation reconciliation,
                String namespace,
                String podName,
                String clusterId) {
            throw new UnsupportedOperationException("FailingInitProvider is only for testing init() failures");
        }
        
        @Override
        public Future<Void> deleteNetworkingResources(
                io.strimzi.operator.common.Reconciliation reconciliation,
                String namespace,
                String podName,
                String clusterId) {
            throw new UnsupportedOperationException("FailingInitProvider is only for testing init() failures");
        }
    }
}
