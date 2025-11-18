/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.operator.cluster.stretch;

import io.fabric8.kubernetes.api.model.apiextensions.v1.CustomResourceDefinition;
import io.fabric8.kubernetes.api.model.apiextensions.v1.CustomResourceDefinitionBuilder;
import io.fabric8.kubernetes.client.KubernetesClient;
import io.strimzi.api.kafka.model.kafka.Kafka;
import io.strimzi.api.kafka.model.nodepool.KafkaNodePool;
import io.strimzi.operator.cluster.stretch.util.StretchTestFixtures;
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

import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.hamcrest.CoreMatchers.containsString;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.CoreMatchers.nullValue;
import static org.hamcrest.MatcherAssert.assertThat;

/**
 * Tests for StretchClusterValidator.
 * 
 * This test class validates:
 * - Operator configuration validation
 * - Kafka CR and KafkaNodePool configuration validation
 * - Runtime connectivity validation
 * - Error messages and error codes
 * 
 * Test Strategy:
 * - Uses real validator logic (no mocking of validator itself)
 * - Uses MockKube3 for Kubernetes client mocking
 * - Tests both success and failure paths
 * - Validates error messages are user-friendly
 * 
 * @see StretchClusterValidator
 */
@Tag("stretch-cluster")
@Tag("stretch-unit")

@ExtendWith(VertxExtension.class)
public class StretchClusterValidatorTest {
    
    private static final String CENTRAL_CLUSTER_ID = "central";
    private static final String REMOTE_CLUSTER_A_ID = "cluster-a";
    private static final String REMOTE_CLUSTER_B_ID = "cluster-b";
    private static final String NAMESPACE = "test-namespace";
    
    private static Vertx vertx;
    private static MockKube3 mockKube;
    
    private StretchClusterValidator validator;
    private KubernetesClient client;
    
    @BeforeAll
    public static void beforeAll() {
        vertx = Vertx.vertx();
        
        // Create mock Kubernetes cluster once for all tests
        mockKube = new MockKube3.MockKube3Builder().build();
        mockKube.start();
    }
    
    @AfterAll
    public static void afterAll() {
        if (mockKube != null) {
            mockKube.stop();
        }
        if (vertx != null) {
            vertx.close();
        }
    }
    
    @BeforeEach
    public void setUp() {
        validator = new StretchClusterValidator(
            vertx,
            CENTRAL_CLUSTER_ID,
            Set.of(REMOTE_CLUSTER_A_ID, REMOTE_CLUSTER_B_ID)
        );
        
        // Get fresh client for each test
        client = mockKube.client();
        
        // Clean up any CRDs from previous tests
        try {
            client.apiextensions().v1().customResourceDefinitions()
                .withName("strimzipodsets.core.strimzi.io").delete();
        } catch (Exception e) {
            // CRD doesn't exist, that's fine
        }
        
        // Ensure namespace exists (prepareNamespace is idempotent in MockKube3)
        try {
            mockKube.prepareNamespace(NAMESPACE);
        } catch (Exception e) {
            // Namespace already exists, that's fine
        }
    }
    
    // ========== Operator Configuration Validation Tests ==========
    
    
    public void testValidateOperatorConfigurationSuccess() {

        boolean isStretchMode = true;
        boolean isOperatorConfigured = true;
        

        StretchClusterValidator.ValidationResult result = 
            validator.validateOperatorConfiguration(isStretchMode, isOperatorConfigured, "my-cluster");
        

        assertThat("Validation should succeed", result.isValid(), is(true));
        assertThat("Error message should be null", result.getErrorMessage(), is(nullValue()));
        assertThat("Error code should be null", result.getErrorCode(), is(nullValue()));
    }
    
    
    public void testValidateOperatorConfigurationNotStretchMode() {

        boolean isStretchMode = false;
        boolean isOperatorConfigured = false;
        

        StretchClusterValidator.ValidationResult result = 
            validator.validateOperatorConfiguration(isStretchMode, isOperatorConfigured, "my-cluster");
        

        assertThat("Validation should succeed when stretch mode is disabled", 
                  result.isValid(), is(true));
    }
    
    
    public void testValidateOperatorConfigurationMissingConfiguration() {

        boolean isStretchMode = true;
        boolean isOperatorConfigured = false;
        String kafkaName = "my-cluster";
        

        StretchClusterValidator.ValidationResult result = 
            validator.validateOperatorConfiguration(isStretchMode, isOperatorConfigured, kafkaName);
        

        assertThat("Validation should fail", result.isValid(), is(false));
        assertThat("Error code should be OperatorNotConfigured", 
                  result.getErrorCode(), is("OperatorNotConfigured"));
        assertThat("Error message should mention required env vars", 
                  result.getErrorMessage(), containsString("STRIMZI_REMOTE_KUBE_CONFIG"));
        assertThat("Error message should mention cluster name", 
                  result.getErrorMessage(), containsString(kafkaName));
    }
    
    // ========== Kafka Configuration Validation Tests ==========
    
    
    public void testValidateKafkaConfigurationSuccess() {

        Kafka kafka = StretchTestFixtures.createStretchKafka();
        List<KafkaNodePool> nodePools = List.of(
            StretchTestFixtures.createRemoteNodePool("central-pool", 3, CENTRAL_CLUSTER_ID),
            StretchTestFixtures.createRemoteNodePool("remote-pool-a", 2, REMOTE_CLUSTER_A_ID)
        );
        boolean isStretchMode = true;
        

        StretchClusterValidator.ValidationResult result = 
            validator.validateKafkaConfiguration(kafka, nodePools, isStretchMode);
        

        assertThat("Validation should succeed for valid stretch configuration", 
                  result.isValid(), is(true));
    }
    
    
    public void testValidateKafkaConfigurationMissingStretchAnnotation() {

        Kafka kafka = StretchTestFixtures.createBasicKafka(); // No stretch annotation
        List<KafkaNodePool> nodePools = List.of(
            StretchTestFixtures.createRemoteNodePool("remote-pool", 2, REMOTE_CLUSTER_A_ID)
        );
        boolean isStretchMode = true;
        

        StretchClusterValidator.ValidationResult result = 
            validator.validateKafkaConfiguration(kafka, nodePools, isStretchMode);
        

        assertThat("Validation should fail", result.isValid(), is(false));
        assertThat("Error code should be MissingStretchAnnotation", 
                  result.getErrorCode(), is("MissingStretchAnnotation"));
        assertThat("Error message should mention missing annotation", 
                  result.getErrorMessage(), containsString("strimzi.io/enable-stretch-cluster"));
    }
    
    
    public void testValidateKafkaConfigurationNodePoolsWithoutTargetCluster() {

        Kafka kafka = StretchTestFixtures.createStretchKafka();
        List<KafkaNodePool> nodePools = List.of(
            StretchTestFixtures.createCentralNodePool("pool-without-target", 3) // No cluster alias
        );
        boolean isStretchMode = true;
        

        StretchClusterValidator.ValidationResult result = 
            validator.validateKafkaConfiguration(kafka, nodePools, isStretchMode);
        

        assertThat("Validation should fail", result.isValid(), is(false));
        assertThat("Error code should be MissingTargetCluster", 
                  result.getErrorCode(), is("MissingTargetCluster"));
        assertThat("Error message should mention missing annotation", 
                  result.getErrorMessage(), containsString("strimzi.io/stretch-cluster-alias"));
        assertThat("Error message should mention pool name", 
                  result.getErrorMessage(), containsString("pool-without-target"));
    }
    
    
    public void testValidateKafkaConfigurationInvalidTargetCluster() {

        Kafka kafka = StretchTestFixtures.createStretchKafka();
        List<KafkaNodePool> nodePools = List.of(
            StretchTestFixtures.createRemoteNodePool("invalid-pool", 2, "non-existent-cluster")
        );
        boolean isStretchMode = true;
        

        StretchClusterValidator.ValidationResult result = 
            validator.validateKafkaConfiguration(kafka, nodePools, isStretchMode);
        

        assertThat("Validation should fail", result.isValid(), is(false));
        assertThat("Error code should be InvalidTargetCluster", 
                  result.getErrorCode(), is("InvalidTargetCluster"));
        assertThat("Error message should mention invalid cluster ID", 
                  result.getErrorMessage(), containsString("non-existent-cluster"));
        assertThat("Error message should list valid cluster IDs", 
                  result.getErrorMessage(), containsString(CENTRAL_CLUSTER_ID));
    }
    
    
    public void testValidateKafkaConfigurationOperatorNotConfiguredButNodePoolsHaveAnnotations() {

        Kafka kafka = StretchTestFixtures.createBasicKafka();
        List<KafkaNodePool> nodePools = List.of(
            StretchTestFixtures.createRemoteNodePool("remote-pool", 2, REMOTE_CLUSTER_A_ID)
        );
        boolean isStretchMode = false; // Operator not configured
        

        StretchClusterValidator.ValidationResult result = 
            validator.validateKafkaConfiguration(kafka, nodePools, isStretchMode);
        

        assertThat("Validation should fail", result.isValid(), is(false));
        assertThat("Error code should be OperatorNotConfigured", 
                  result.getErrorCode(), is("OperatorNotConfigured"));
        assertThat("Error message should mention operator not configured", 
                  result.getErrorMessage(), containsString("operator is not configured"));
        assertThat("Error message should mention required env vars", 
                  result.getErrorMessage(), containsString("STRIMZI_REMOTE_KUBE_CONFIG"));
    }
    
    @Test
    public void testValidateKafkaConfigurationNonStretchMode() {

        Kafka kafka = StretchTestFixtures.createBasicKafka();
        List<KafkaNodePool> nodePools = List.of(
            StretchTestFixtures.createCentralNodePool("central-pool", 3)
        );
        boolean isStretchMode = false;
        

        StretchClusterValidator.ValidationResult result = 
            validator.validateKafkaConfiguration(kafka, nodePools, isStretchMode);
        

        assertThat("Validation should succeed for non-stretch mode", 
                  result.isValid(), is(true));
    }
    
    
    public void testValidateKafkaConfigurationMultipleNodePoolsAllValid() {

        Kafka kafka = StretchTestFixtures.createStretchKafka();
        List<KafkaNodePool> nodePools = List.of(
            StretchTestFixtures.createRemoteNodePool("central-pool", 3, CENTRAL_CLUSTER_ID),
            StretchTestFixtures.createRemoteNodePool("remote-pool-a", 2, REMOTE_CLUSTER_A_ID),
            StretchTestFixtures.createRemoteNodePool("remote-pool-b", 2, REMOTE_CLUSTER_B_ID)
        );
        boolean isStretchMode = true;
        

        StretchClusterValidator.ValidationResult result = 
            validator.validateKafkaConfiguration(kafka, nodePools, isStretchMode);
        

        assertThat("Validation should succeed with multiple valid pools", 
                  result.isValid(), is(true));
    }
    
    
    public void testValidateKafkaConfigurationMixedValidAndInvalidPools() {

        Kafka kafka = StretchTestFixtures.createStretchKafka();
        List<KafkaNodePool> nodePools = List.of(
            StretchTestFixtures.createRemoteNodePool("central-pool", 3, CENTRAL_CLUSTER_ID),
            StretchTestFixtures.createCentralNodePool("pool-without-annotation", 2) // Missing annotation
        );
        boolean isStretchMode = true;
        

        StretchClusterValidator.ValidationResult result = 
            validator.validateKafkaConfiguration(kafka, nodePools, isStretchMode);
        

        assertThat("Validation should fail when some pools missing annotation", 
                  result.isValid(), is(false));
        assertThat("Error code should be MissingTargetCluster", 
                  result.getErrorCode(), is("MissingTargetCluster"));
    }
    
    // ========== Runtime Connectivity Validation Tests ==========
    
    
    public void testValidateRuntimeConnectivitySuccess(VertxTestContext context) {

        createStrimziPodSetCRD();
        Map<String, KubernetesClient> clusterClients = Map.of(
            CENTRAL_CLUSTER_ID, client
        );

        validator.validateRuntimeConnectivity(clusterClients)
            .onComplete(context.succeeding(result -> context.verify(() -> {
        
                assertThat("Connectivity validation should succeed", 
                          result.isValid(), is(true));
                assertThat("Error message should be null", 
                          result.getErrorMessage(), is(nullValue()));
                
                context.completeNow();
            })));
    }
    
    
    public void testValidateRuntimeConnectivityMissingCRD(VertxTestContext context) {
        // Don't create CRD - test should fail due to missing CRD
        Map<String, KubernetesClient> clusterClients = Map.of(
            CENTRAL_CLUSTER_ID, client
        );

        validator.validateRuntimeConnectivity(clusterClients)
            .onComplete(context.succeeding(result -> context.verify(() -> {
        
                assertThat("Connectivity validation should fail", 
                          result.isValid(), is(false));
                assertThat("Error code should be MissingCRD", 
                          result.getErrorCode(), is("MissingCRD"));
                assertThat("Error message should mention missing CRD", 
                          result.getErrorMessage(), containsString("strimzipodsets.core.strimzi.io"));
                assertThat("Error message should mention cluster ID", 
                          result.getErrorMessage(), containsString(CENTRAL_CLUSTER_ID));
                
                context.completeNow();
            })));
    }
    
    
    public void testValidateRuntimeConnectivityMultipleClusters(VertxTestContext context) {

        createStrimziPodSetCRD();
        
        // Create additional mock clients for remote clusters
        MockKube3 mockKubeRemoteA = new MockKube3.MockKube3Builder()
                .build();
        mockKubeRemoteA.start();
        KubernetesClient remoteClientA = mockKubeRemoteA.client();
        createStrimziPodSetCRD(remoteClientA);
        
        Map<String, KubernetesClient> clusterClients = Map.of(
            CENTRAL_CLUSTER_ID, client,
            REMOTE_CLUSTER_A_ID, remoteClientA
        );

        validator.validateRuntimeConnectivity(clusterClients)
            .onComplete(context.succeeding(result -> context.verify(() -> {
        
                assertThat("Connectivity validation should succeed for multiple clusters", 
                          result.isValid(), is(true));
                
                // Cleanup
                mockKubeRemoteA.stop();
                context.completeNow();
            })));
    }
    
    // ========== ValidationResult Tests ==========
    
    @Test
    public void testValidationResultSuccess() {

        StretchClusterValidator.ValidationResult result = 
            StretchClusterValidator.ValidationResult.success();
        

        assertThat("Result should be valid", result.isValid(), is(true));
        assertThat("Error message should be null", result.getErrorMessage(), is(nullValue()));
        assertThat("Error code should be null", result.getErrorCode(), is(nullValue()));
    }
    
    @Test
    public void testValidationResultError() {

        String errorCode = "TestError";
        String errorMessage = "This is a test error message";
        

        StretchClusterValidator.ValidationResult result = 
            StretchClusterValidator.ValidationResult.error(errorCode, errorMessage);
        

        assertThat("Result should be invalid", result.isValid(), is(false));
        assertThat("Error message should match", result.getErrorMessage(), is(errorMessage));
        assertThat("Error code should match", result.getErrorCode(), is(errorCode));
    }
    
    // ========== Helper Methods ==========
    
    /**
     * Creates StrimziPodSet CRD in the mock Kubernetes cluster.
     */
    private void createStrimziPodSetCRD() {
        createStrimziPodSetCRD(client);
    }
    
    /**
     * Creates StrimziPodSet CRD in the specified Kubernetes client.
     */
    private void createStrimziPodSetCRD(KubernetesClient kubeClient) {
        CustomResourceDefinition crd = new CustomResourceDefinitionBuilder()
                .withNewMetadata()
                    .withName("strimzipodsets.core.strimzi.io")
                .endMetadata()
                .withNewSpec()
                    .withGroup("core.strimzi.io")
                    .withNewNames()
                        .withKind("StrimziPodSet")
                        .withPlural("strimzipodsets")
                        .withSingular("strimzipodset")
                    .endNames()
                    .withScope("Namespaced")
                    .addNewVersion()
                        .withName("v1beta2")
                        .withServed(true)
                        .withStorage(true)
                        .withNewSchema()
                            .withNewOpenAPIV3Schema()
                                .withType("object")
                            .endOpenAPIV3Schema()
                        .endSchema()
                    .endVersion()
                .endSpec()
                .build();
        
        kubeClient.apiextensions().v1().customResourceDefinitions().resource(crd).create();
    }
}
