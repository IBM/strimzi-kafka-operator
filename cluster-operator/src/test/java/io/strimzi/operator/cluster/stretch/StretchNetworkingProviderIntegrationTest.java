/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.operator.cluster.stretch;

import io.fabric8.kubernetes.api.model.HasMetadata;
import io.fabric8.kubernetes.api.model.Service;
import io.fabric8.kubernetes.client.KubernetesClient;
import io.strimzi.operator.cluster.ClusterOperatorConfig;
import io.strimzi.operator.cluster.KafkaVersionTestUtils;
import io.strimzi.operator.cluster.PlatformFeaturesAvailability;
import io.strimzi.operator.cluster.RemoteClientSupplier;
import io.strimzi.operator.cluster.ResourceUtils;
import io.strimzi.operator.cluster.operator.resource.ResourceOperatorSupplier;
import io.strimzi.operator.cluster.stretch.spi.StretchNetworkingProvider;
import io.strimzi.operator.common.Reconciliation;
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

import static org.hamcrest.CoreMatchers.containsString;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.CoreMatchers.notNullValue;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.hasSize;

/**
 * Integration tests for StretchNetworkingProvider plugin loading and functionality.
 * 
 * These tests validate the complete plugin lifecycle:
 * - Loading plugin from classpath
 * - Initializing provider with suppliers
 * - Creating networking resources
 * - Discovering endpoints
 * - Generating Kafka configuration
 * - Deleting resources
 * 
 * 
 * @see StretchNetworkingProviderFactory
 * @see TestNetworkingProvider
 */
@Tag("stretch-cluster")
@Tag("stretch-integration")
@ExtendWith(VertxExtension.class)
public class StretchNetworkingProviderIntegrationTest {
    
    private static final String CENTRAL_CLUSTER_ID = "central";
    private static final String REMOTE_CLUSTER_A_ID = "cluster-a";
    private static final String REMOTE_CLUSTER_B_ID = "cluster-b";
    private static final String TEST_NAMESPACE = "kafka-ns";
    private static final String TEST_CLUSTER_NAME = "my-cluster";
    
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
        
        // Create mock Kubernetes clusters
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
        // Get fresh clients for each test
        centralClient = mockKubeCentral.client();
        remoteClientA = mockKubeRemoteA.client();
        remoteClientB = mockKubeRemoteB.client();
        
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
        
        RemoteClientSupplier remoteClientSupplier = new RemoteClientSupplier(TEST_NAMESPACE, remoteClusters, remoteClients);
        
        Map<String, PlatformFeaturesAvailability> remotePfas = new HashMap<>();
        remotePfas.put(REMOTE_CLUSTER_A_ID, pfa);
        remotePfas.put(REMOTE_CLUSTER_B_ID, pfa);
        
        remoteSupplier = new RemoteResourceOperatorSupplier(
            vertx, centralClient, remoteClientSupplier, remotePfas, "test-operator", CENTRAL_CLUSTER_ID
        );
    }
    
    // ========== Plugin Loading Tests ==========
    
    @Test
    public void testLoadPluginFromClasspath(VertxTestContext context) {

        ClusterOperatorConfig config = createConfig(
            "io.strimzi.operator.cluster.stretch.TestNetworkingProvider",
            ""
        );
        Map<String, String> providerConfig = new HashMap<>();
        providerConfig.put("test.key", "test.value");
        

        context.verify(() -> {
            StretchNetworkingProvider provider = StretchNetworkingProviderFactory.create(
                config, providerConfig, centralSupplier, remoteSupplier
            );
            
            assertThat("Provider should be loaded", provider, is(notNullValue()));
            assertThat("Provider should be TestNetworkingProvider", 
                      provider instanceof TestNetworkingProvider, is(true));
            assertThat("Provider name should be correct", 
                      provider.getProviderName(), is("test-provider"));
            
 
            TestNetworkingProvider testProvider = (TestNetworkingProvider) provider;
            assertThat("Provider should be initialized", testProvider.isInitialized(), is(true));
            assertThat("Config should be passed", testProvider.getConfig(), is(notNullValue()));
            assertThat("Config should contain test key", 
                      testProvider.getConfig().get("test.key"), is("test.value"));
            
            context.completeNow();
        });
    }
    
    // ========== Provider Initialization Tests ==========
    
    @Test
    public void testProviderInitializationWithSuppliers(VertxTestContext context) {

        ClusterOperatorConfig config = createConfig(
            "io.strimzi.operator.cluster.stretch.TestNetworkingProvider",
            ""
        );
        

        StretchNetworkingProvider provider = StretchNetworkingProviderFactory.create(
            config, new HashMap<>(), centralSupplier, remoteSupplier
        );
        

        context.verify(() -> {
            TestNetworkingProvider testProvider = (TestNetworkingProvider) provider;
            assertThat("Provider should be initialized", testProvider.isInitialized(), is(true));
            
            context.completeNow();
        });
    }
    
    // ========== Resource Creation Tests ==========
    
    @Test
    public void testCreateNetworkingResources(VertxTestContext context) {

        ClusterOperatorConfig config = createConfig(
            "io.strimzi.operator.cluster.stretch.TestNetworkingProvider",
            ""
        );
        StretchNetworkingProvider provider = StretchNetworkingProviderFactory.create(
            config, new HashMap<>(), centralSupplier, remoteSupplier
        );
        
        Reconciliation reconciliation = new Reconciliation("test", "Kafka", TEST_NAMESPACE, TEST_CLUSTER_NAME);
        String podName = TEST_CLUSTER_NAME + "-kafka-0";
        Map<String, Integer> ports = new HashMap<>();
        ports.put("replication", 9091);
        ports.put("controller", 9093);
        

        provider.createNetworkingResources(reconciliation, TEST_NAMESPACE, podName, CENTRAL_CLUSTER_ID, ports)
            .onComplete(context.succeeding(resources -> context.verify(() -> {
        
                assertThat("Should create resources", resources, is(notNullValue()));
                assertThat("Should create at least one resource", resources.size(), greaterThan(0));
                
                // Verify service was created
                HasMetadata resource = resources.get(0);
                assertThat("Resource should be a Service", resource instanceof Service, is(true));
                
                Service service = (Service) resource;
                assertThat("Service name should be correct", 
                          service.getMetadata().getName(), containsString(podName));
                assertThat("Service should have ports", 
                          service.getSpec().getPorts(), hasSize(2));
                
                context.completeNow();
            })));
    }
    
    // ========== Endpoint Discovery Tests ==========
    
    @Test
    public void testDiscoverPodEndpoint(VertxTestContext context) {

        ClusterOperatorConfig config = createConfig(
            "io.strimzi.operator.cluster.stretch.TestNetworkingProvider",
            ""
        );
        StretchNetworkingProvider provider = StretchNetworkingProviderFactory.create(
            config, new HashMap<>(), centralSupplier, remoteSupplier
        );
        
        Reconciliation reconciliation = new Reconciliation("test", "Kafka", TEST_NAMESPACE, TEST_CLUSTER_NAME);
        String serviceName = TEST_CLUSTER_NAME + "-kafka-0-svc";
        

        provider.discoverPodEndpoint(reconciliation, TEST_NAMESPACE, serviceName, CENTRAL_CLUSTER_ID, "replication")
            .onComplete(context.succeeding(endpoint -> context.verify(() -> {
        
                assertThat("Endpoint should be returned", endpoint, is(notNullValue()));
                assertThat("Endpoint should contain service name", endpoint, containsString(serviceName));
                assertThat("Endpoint should contain namespace", endpoint, containsString(TEST_NAMESPACE));
                assertThat("Endpoint should contain port", endpoint, containsString(":"));
                
                context.completeNow();
            })));
    }
    
    // ========== DNS Name Generation Tests ==========
    
    @Test
    public void testGenerateServiceDnsName() {

        ClusterOperatorConfig config = createConfig(
            "io.strimzi.operator.cluster.stretch.TestNetworkingProvider",
            ""
        );
        StretchNetworkingProvider provider = StretchNetworkingProviderFactory.create(
            config, new HashMap<>(), centralSupplier, remoteSupplier
        );
        
        String serviceName = TEST_CLUSTER_NAME + "-kafka-bootstrap";
        

        String dnsName = provider.generateServiceDnsName(TEST_NAMESPACE, serviceName, CENTRAL_CLUSTER_ID);
        

        assertThat("DNS name should be generated", dnsName, is(notNullValue()));
        assertThat("DNS name should contain service name", dnsName, containsString(serviceName));
        assertThat("DNS name should contain namespace", dnsName, containsString(TEST_NAMESPACE));
        assertThat("DNS name should be FQDN", dnsName, containsString(".svc.cluster.local"));
    }
    
    @Test
    public void testGeneratePodDnsName() {

        ClusterOperatorConfig config = createConfig(
            "io.strimzi.operator.cluster.stretch.TestNetworkingProvider",
            ""
        );
        StretchNetworkingProvider provider = StretchNetworkingProviderFactory.create(
            config, new HashMap<>(), centralSupplier, remoteSupplier
        );
        
        String podName = TEST_CLUSTER_NAME + "-kafka-0";
        String serviceName = TEST_CLUSTER_NAME + "-kafka-0-svc";
        

        String dnsName = provider.generatePodDnsName(TEST_NAMESPACE, serviceName, podName, CENTRAL_CLUSTER_ID);
        

        assertThat("DNS name should be generated", dnsName, is(notNullValue()));
        assertThat("DNS name should contain pod name", dnsName, containsString(podName));
        assertThat("DNS name should contain namespace", dnsName, containsString(TEST_NAMESPACE));
        assertThat("DNS name should be FQDN", dnsName, containsString(".svc.cluster.local"));
    }
    
    // ========== Kafka Configuration Generation Tests ==========
    
    @Test
    public void testGenerateAdvertisedListeners(VertxTestContext context) {

        ClusterOperatorConfig config = createConfig(
            "io.strimzi.operator.cluster.stretch.TestNetworkingProvider",
            ""
        );
        StretchNetworkingProvider provider = StretchNetworkingProviderFactory.create(
            config, new HashMap<>(), centralSupplier, remoteSupplier
        );
        
        Reconciliation reconciliation = new Reconciliation("test", "Kafka", TEST_NAMESPACE, TEST_CLUSTER_NAME);
        String podName = TEST_CLUSTER_NAME + "-kafka-0";
        Map<String, String> listeners = new HashMap<>();
        listeners.put("REPLICATION-9091", "replication");
        listeners.put("PLAIN-9092", "plain");
        

        provider.generateAdvertisedListeners(reconciliation, TEST_NAMESPACE, podName, CENTRAL_CLUSTER_ID, listeners)
            .onComplete(context.succeeding(advertisedListeners -> context.verify(() -> {
        
                assertThat("Advertised listeners should be generated", advertisedListeners, is(notNullValue()));
                assertThat("Should contain REPLICATION listener", 
                          advertisedListeners, containsString("REPLICATION-9091://"));
                assertThat("Should contain PLAIN listener", 
                          advertisedListeners, containsString("PLAIN-9092://"));
                assertThat("Should contain DNS name", 
                          advertisedListeners, containsString(".svc.cluster.local"));
                
                context.completeNow();
            })));
    }
    
    @Test
    public void testGenerateQuorumVoters(VertxTestContext context) {

        ClusterOperatorConfig config = createConfig(
            "io.strimzi.operator.cluster.stretch.TestNetworkingProvider",
            ""
        );
        StretchNetworkingProvider provider = StretchNetworkingProviderFactory.create(
            config, new HashMap<>(), centralSupplier, remoteSupplier
        );
        
        Reconciliation reconciliation = new Reconciliation("test", "Kafka", TEST_NAMESPACE, TEST_CLUSTER_NAME);
        List<StretchNetworkingProvider.ControllerPodInfo> controllerPods = List.of(
            new StretchNetworkingProvider.ControllerPodInfo(0, TEST_CLUSTER_NAME + "-kafka-0", CENTRAL_CLUSTER_ID),
            new StretchNetworkingProvider.ControllerPodInfo(1, TEST_CLUSTER_NAME + "-kafka-1", REMOTE_CLUSTER_A_ID),
            new StretchNetworkingProvider.ControllerPodInfo(2, TEST_CLUSTER_NAME + "-kafka-2", REMOTE_CLUSTER_B_ID)
        );
        

        provider.generateQuorumVoters(reconciliation, TEST_NAMESPACE, controllerPods, "replication")
            .onComplete(context.succeeding(quorumVoters -> context.verify(() -> {
        
                assertThat("Quorum voters should be generated", quorumVoters, is(notNullValue()));
                assertThat("Should contain node 0", quorumVoters, containsString("0@"));
                assertThat("Should contain node 1", quorumVoters, containsString("1@"));
                assertThat("Should contain node 2", quorumVoters, containsString("2@"));
                assertThat("Should contain DNS names", quorumVoters, containsString(".svc.cluster.local"));
                assertThat("Should be comma-separated", quorumVoters, containsString(","));
                
                context.completeNow();
            })));
    }
    
    // ========== Certificate SAN Generation Tests ==========
    
    @Test
    public void testGenerateCertificateSans(VertxTestContext context) {

        ClusterOperatorConfig config = createConfig(
            "io.strimzi.operator.cluster.stretch.TestNetworkingProvider",
            ""
        );
        StretchNetworkingProvider provider = StretchNetworkingProviderFactory.create(
            config, new HashMap<>(), centralSupplier, remoteSupplier
        );
        
        Reconciliation reconciliation = new Reconciliation("test", "Kafka", TEST_NAMESPACE, TEST_CLUSTER_NAME);
        String podName = TEST_CLUSTER_NAME + "-kafka-0";
        

        provider.generateCertificateSans(reconciliation, TEST_NAMESPACE, podName, CENTRAL_CLUSTER_ID)
            .onComplete(context.succeeding(sans -> context.verify(() -> {
        
                assertThat("SANs should be generated", sans, is(notNullValue()));
                assertThat("Should have at least one SAN", sans.size(), greaterThan(0));
                assertThat("SANs should contain DNS names", 
                          sans.get(0), containsString(".svc.cluster.local"));
                
                context.completeNow();
            })));
    }
    
    // ========== Resource Deletion Tests ==========
    
    @Test
    public void testDeleteNetworkingResources(VertxTestContext context) {

        ClusterOperatorConfig config = createConfig(
            "io.strimzi.operator.cluster.stretch.TestNetworkingProvider",
            ""
        );
        StretchNetworkingProvider provider = StretchNetworkingProviderFactory.create(
            config, new HashMap<>(), centralSupplier, remoteSupplier
        );
        
        Reconciliation reconciliation = new Reconciliation("test", "Kafka", TEST_NAMESPACE, TEST_CLUSTER_NAME);
        String podName = TEST_CLUSTER_NAME + "-kafka-0";
        

        provider.deleteNetworkingResources(reconciliation, TEST_NAMESPACE, podName, CENTRAL_CLUSTER_ID)
            .onComplete(context.succeeding(v -> context.verify(() -> {

                context.completeNow();
            })));
    }
    
    // ========== Multi-Cluster Tests ==========
    
    @Test
    public void testProviderWorksWithMultipleClusters(VertxTestContext context) {

        ClusterOperatorConfig config = createConfig(
            "io.strimzi.operator.cluster.stretch.TestNetworkingProvider",
            ""
        );
        StretchNetworkingProvider provider = StretchNetworkingProviderFactory.create(
            config, new HashMap<>(), centralSupplier, remoteSupplier
        );
        
        Reconciliation reconciliation = new Reconciliation("test", "Kafka", TEST_NAMESPACE, TEST_CLUSTER_NAME);

        Map<String, Integer> ports = Map.of("replication", 9091);
        
        provider.createNetworkingResources(reconciliation, TEST_NAMESPACE, 
                TEST_CLUSTER_NAME + "-kafka-0", CENTRAL_CLUSTER_ID, ports)
            .compose(r1 -> provider.createNetworkingResources(reconciliation, TEST_NAMESPACE, 
                TEST_CLUSTER_NAME + "-kafka-1", REMOTE_CLUSTER_A_ID, ports))
            .compose(r2 -> provider.createNetworkingResources(reconciliation, TEST_NAMESPACE, 
                TEST_CLUSTER_NAME + "-kafka-2", REMOTE_CLUSTER_B_ID, ports))
            .onComplete(context.succeeding(resources -> context.verify(() -> {
                assertThat("Resources should be created", resources, is(notNullValue()));
                context.completeNow();
            })));
    }
    
    // ========== Helper Methods ==========
    
    private ClusterOperatorConfig createConfig(String className, String classPath) {
        ClusterOperatorConfig.ClusterOperatorConfigBuilder builder = 
            new ClusterOperatorConfig.ClusterOperatorConfigBuilder(
                ResourceUtils.dummyClusterOperatorConfig(), 
                KafkaVersionTestUtils.getKafkaVersionLookup())
            .with("STRIMZI_CENTRAL_CLUSTER_ID", CENTRAL_CLUSTER_ID)
            .with("STRIMZI_REMOTE_KUBE_CONFIG", 
                REMOTE_CLUSTER_A_ID + ".url=https://api.cluster-a.example.com:6443\n" +
                REMOTE_CLUSTER_A_ID + ".secret=cluster-a-kubeconfig\n" +
                REMOTE_CLUSTER_B_ID + ".url=https://api.cluster-b.example.com:6443\n" +
                REMOTE_CLUSTER_B_ID + ".secret=cluster-b-kubeconfig")
            .with("STRIMZI_STRETCH_NETWORK_PROVIDER", "custom"); // Required - no defaults
        
        if (className != null) {
            builder.with("STRIMZI_STRETCH_PLUGIN_CLASS_NAME", className);
        }
        if (classPath != null) {
            builder.with("STRIMZI_STRETCH_PLUGIN_CLASS_PATH", classPath);
        }
        
        return builder.build();
    }
}
