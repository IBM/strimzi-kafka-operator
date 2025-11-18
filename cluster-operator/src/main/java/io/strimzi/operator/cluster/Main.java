/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.operator.cluster;

import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import io.fabric8.kubernetes.client.KubernetesClient;
import io.micrometer.prometheusmetrics.PrometheusMeterRegistry;
import io.strimzi.certs.OpenSslCertManager;
import io.strimzi.operator.cluster.leaderelection.LeaderElectionManager;
import io.strimzi.operator.cluster.model.securityprofiles.PodSecurityProviderFactory;
import io.strimzi.operator.cluster.operator.assembly.KafkaAssemblyOperator;
import io.strimzi.operator.cluster.operator.assembly.KafkaBridgeAssemblyOperator;
import io.strimzi.operator.cluster.operator.assembly.KafkaConnectAssemblyOperator;
import io.strimzi.operator.cluster.operator.assembly.KafkaMirrorMaker2AssemblyOperator;
import io.strimzi.operator.cluster.operator.assembly.KafkaRebalanceAssemblyOperator;
import io.strimzi.operator.cluster.operator.resource.ResourceOperatorSupplier;
import io.strimzi.operator.cluster.operator.resource.kubernetes.SecretOperator;
import io.strimzi.operator.cluster.stretch.RemoteResourceOperatorSupplier;
import io.strimzi.operator.cluster.stretch.StretchInitializer;
import io.strimzi.operator.common.MetricsProvider;
import io.strimzi.operator.common.MicrometerMetricsProvider;
import io.strimzi.operator.common.OperatorKubernetesClientBuilder;
import io.strimzi.operator.common.Util;
import io.strimzi.operator.common.model.PasswordGenerator;
import io.vertx.core.CompositeFuture;
import io.vertx.core.Context;
import io.vertx.core.Future;
import io.vertx.core.Promise;
import io.vertx.core.Vertx;
import io.vertx.core.VertxOptions;
import io.vertx.core.http.HttpServer;
import io.vertx.micrometer.MicrometerMetricsOptions;
import io.vertx.micrometer.VertxPrometheusOptions;
import io.vertx.micrometer.backends.BackendRegistries;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.security.Security;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;

/**
 * The main class used to start the Strimzi Cluster Operator
 */
@SuppressFBWarnings("DM_EXIT")
@SuppressWarnings({"checkstyle:ClassDataAbstractionCoupling"})
public class Main {
    private static final Logger LOGGER = LogManager.getLogger(Main.class.getName());

    private static final int HEALTH_SERVER_PORT = 8080;
    private static final long SHUTDOWN_TIMEOUT = 10_000L;

    /**
     * The main method used to run the Cluster Operator
     *
     * @param args  The command line arguments
     */
    public static void main(String[] args) {
        final String strimziVersion = Main.class.getPackage().getImplementationVersion();
        LOGGER.info("ClusterOperator {} is starting", strimziVersion);
        Util.printEnvInfo(); // Prints configured environment variables
        ClusterOperatorConfig config = ClusterOperatorConfig.buildFromMap(System.getenv());
        LOGGER.info("Cluster Operator configuration is {}", config);

        // setting DNS cache TTL
        Security.setProperty("networkaddress.cache.ttl", String.valueOf(config.getDnsCacheTtlSec()));

        // Shutdown hook to register shutdown actions
        ShutdownHook shutdownHook = new ShutdownHook();
        Runtime.getRuntime().addShutdownHook(new Thread(shutdownHook));

        // setup Micrometer metrics options
        VertxOptions options = new VertxOptions().setMetricsOptions(
            new MicrometerMetricsOptions()
                .setPrometheusOptions(new VertxPrometheusOptions().setEnabled(true))
                .setJvmMetricsEnabled(true)
                .setEnabled(true));
        Vertx vertx = Vertx.vertx(options);
        shutdownHook.register(() -> ShutdownHook.shutdownVertx(vertx, SHUTDOWN_TIMEOUT));

        // Setup Micrometer Metrics provider
        MetricsProvider metricsProvider = new MicrometerMetricsProvider(BackendRegistries.getDefaultNow());
        KubernetesClient client = new OperatorKubernetesClientBuilder("strimzi-cluster-operator", strimziVersion).build();

        RemoteClientSupplier remoteClientSupplier = RemoteClientSupplier.buildFromClusterInfo(
                config.getOperatorNamespace(),
                config.getRemoteClusters(),
                new SecretOperator(vertx, client)
            );

        startHealthServer(vertx, metricsProvider)
                .compose(i -> leaderElection(client, config, shutdownHook))
                .compose(i -> createPlatformFeaturesAvailability(vertx, client))
                .compose(centralPfa -> {
                    // Initialize stretch cluster functionality
                    return StretchInitializer.initialize(config, vertx, client, remoteClientSupplier, centralPfa)
                            .compose(stretchResult -> {
                                return deployClusterOperatorVerticles(
                                    vertx,
                                    client,
                                    remoteClientSupplier,
                                    metricsProvider,
                                    centralPfa,
                                    config,
                                    shutdownHook,
                                    stretchResult.getRemotePfas(),
                                    stretchResult.getRemoteResourceOperatorSupplier(),
                                    stretchResult.envConfigsValid(),
                                    stretchResult.remoteKubeConfigsValid()
                                );
                            })
                            .mapEmpty();
                })
                .onComplete(res -> {
                    if (res.failed())   {
                        handleStartupFailure(res.cause(), vertx);
                    }
                });
    }

    /**
     * Handles startup failures with appropriate error messages and exits the operator.
     * Provides specific guidance for authentication failures in stretch cluster configurations.
     *
     * @param cause The exception that caused the startup failure
     * @param vertx Vertx instance for executing the shutdown
     */
    private static void handleStartupFailure(Throwable cause, Vertx vertx) {
        if (isAuthenticationFailure(cause)) {
            logStretchClusterAuthenticationError(cause);
        } else {
            LOGGER.error("Unable to start operator for 1 or more namespace", cause);
        }

        vertx.executeBlocking(() -> {
            System.exit(1);
            return true;
        });
    }

    /**
     * Checks if the failure is due to authentication issues with remote clusters.
     *
     * @param cause The exception to check
     * @return true if this is an authentication failure, false otherwise
     */
    private static boolean isAuthenticationFailure(Throwable cause) {
        return cause instanceof IllegalStateException &&
               cause.getMessage() != null &&
               cause.getMessage().contains("Authentication failed for remote cluster");
    }

    /**
     * Logs a detailed error message for stretch cluster authentication failures.
     * Provides actionable guidance for resolving expired or invalid kubeconfig secrets.
     *
     * @param cause The authentication failure exception
     */
    private static void logStretchClusterAuthenticationError(Throwable cause) {
        LOGGER.error("Failed to start Cluster Operator due to invalid remote cluster credentials: {}\n" +
                     "The kubeconfig secret referenced in STRIMZI_REMOTE_KUBE_CONFIG appears to be invalid or expired.\n" +
                     "To resolve this issue:\n" +
                     "  1. Verify the secret contains valid, non-expired credentials\n" +
                     "  2. Update the secret with fresh credentials if necessary\n" +
                     "Note: Short-lived tokens (e.g., 24-hour tokens) require periodic renewal. " +
                     "Consider using long-lived service account tokens or certificate-based authentication for production environments.",
                     cause.getMessage());
    }

    /**
     * Helper method used to get the PlatformFeaturesAvailability instance with the information about the Kubernetes
     * cluster we run on.
     *
     * @param vertx     Vertx instance
     * @param client    Kubernetes client instance
     *
     * @return  Future with the created PlatformFeaturesAvailability object
     */
    private static Future<PlatformFeaturesAvailability> createPlatformFeaturesAvailability(Vertx vertx, KubernetesClient client)    {
        Promise<PlatformFeaturesAvailability> promise = Promise.promise();

        PlatformFeaturesAvailability.create(vertx, client).onComplete(pfa -> {
            if (pfa.succeeded()) {
                LOGGER.info("Environment facts gathered: {}", pfa.result());
                promise.complete(pfa.result());
            } else {
                LOGGER.error("Failed to gather environment facts", pfa.cause());
                promise.fail(pfa.cause());
            }
        });

        return promise.future();
    }

    /**
     * Deploys the ClusterOperator verticles responsible for the actual Cluster Operator functionality. One verticle is
     * started for each namespace the operator watched. In case of watching the whole cluster, only one verticle is started.
     *
     * @param vertx             Vertx instance
     * @param client            Kubernetes client instance
     * @param metricsProvider   Metrics provider instance
     * @param pfa               PlatformFeaturesAvailability instance describing the Kubernetes cluster
     * @param config            Cluster Operator configuration
     * @param shutdownHook      Shutdown hook to register leader election shutdown
     *
     * @return  Future which completes when all Cluster Operator verticles are started and running
     */
    static CompositeFuture deployClusterOperatorVerticles(Vertx vertx, KubernetesClient client, RemoteClientSupplier remoteClientSupplier, MetricsProvider metricsProvider, PlatformFeaturesAvailability pfa, ClusterOperatorConfig config, ShutdownHook shutdownHook, Map<String, PlatformFeaturesAvailability> remotePfas, RemoteResourceOperatorSupplier remoteResourceOperatorSupplier, boolean envConfigsValid, boolean remoteKubeConfigsValid) {
        // Create ResourceOperatorSupplier with remote cluster support
        // Pass empty map and null if no remote clusters configured
        
        ResourceOperatorSupplier resourceOperatorSupplier = new ResourceOperatorSupplier(
                vertx,
                client,
                metricsProvider,
                pfa,
                config.getOperatorName(),
                config.featureGates()
        );

        // Initialize the PodSecurityProvider factory to provide the user configured provider
        PodSecurityProviderFactory.initialize(config.getPodSecurityProviderClass(), pfa);

        KafkaAssemblyOperator kafkaClusterOperations = null;
        KafkaConnectAssemblyOperator kafkaConnectClusterOperations = null;
        KafkaMirrorMaker2AssemblyOperator kafkaMirrorMaker2AssemblyOperator = null;
        KafkaBridgeAssemblyOperator kafkaBridgeAssemblyOperator = null;
        KafkaRebalanceAssemblyOperator kafkaRebalanceAssemblyOperator = null;

        if (!config.isPodSetReconciliationOnly()) {
            OpenSslCertManager certManager = new OpenSslCertManager();
            PasswordGenerator passwordGenerator = new PasswordGenerator(12,
                    "abcdefghijklmnopqrstuvwxyz" +
                            "ABCDEFGHIJKLMNOPQRSTUVWXYZ",
                    "abcdefghijklmnopqrstuvwxyz" +
                            "ABCDEFGHIJKLMNOPQRSTUVWXYZ" +
                            "0123456789");

            kafkaClusterOperations = new KafkaAssemblyOperator(vertx, pfa, certManager, passwordGenerator, resourceOperatorSupplier, config);
            // Add stretch capabilities if configured
            if (envConfigsValid) {
                kafkaClusterOperations = kafkaClusterOperations.withStretchCapabilities(remoteResourceOperatorSupplier, remotePfas, remoteKubeConfigsValid);
            }
            kafkaConnectClusterOperations = new KafkaConnectAssemblyOperator(vertx, pfa, resourceOperatorSupplier, config);
            kafkaMirrorMaker2AssemblyOperator = new KafkaMirrorMaker2AssemblyOperator(vertx, pfa, resourceOperatorSupplier, config);
            kafkaBridgeAssemblyOperator = new KafkaBridgeAssemblyOperator(vertx, pfa, certManager, passwordGenerator, resourceOperatorSupplier, config);
            kafkaRebalanceAssemblyOperator = new KafkaRebalanceAssemblyOperator(vertx, resourceOperatorSupplier, config);
        }

        List<Future<String>> futures = new ArrayList<>(config.getNamespaces().size());
        for (String namespace : config.getNamespaces()) {
            Promise<String> prom = Promise.promise();
            futures.add(prom.future());
            ClusterOperator operator = new ClusterOperator(namespace,
                    config,
                    kafkaClusterOperations,
                    kafkaConnectClusterOperations,
                    kafkaMirrorMaker2AssemblyOperator,
                    kafkaBridgeAssemblyOperator,
                    kafkaRebalanceAssemblyOperator,
                    resourceOperatorSupplier);
            vertx.deployVerticle(operator).onComplete(res -> {
                if (res.succeeded()) {
                    shutdownHook.register(() -> ShutdownHook.undeployVertxVerticle(vertx, res.result(), SHUTDOWN_TIMEOUT));

                    if (config.getCustomResourceSelector() != null) {
                        LOGGER.info("Cluster Operator verticle started in namespace {} with label selector {}", namespace, config.getCustomResourceSelector());
                    } else {
                        LOGGER.info("Cluster Operator verticle started in namespace {} without label selector", namespace);
                    }
                } else {
                    LOGGER.error("Cluster Operator verticle in namespace {} failed to start", namespace, res.cause());
                }
                prom.handle(res);
            });
        }
        return Future.join(futures);
    }

    /**
     * Utility method which waits until this instance of the operator is elected as a leader:
     *   - When it is not a leader, it will just wait
     *   - Once it is elected a leader, it will continue and start the ClusterOperator verticles
     *   - If it is removed as a leader, it will loop the operator container to start from the beginning
     *
     * When the leader election is disabled, it just completes the future without waiting for anything.
     *
     * @param client        Kubernetes client
     * @param config        Cluster Operator configuration
     * @param shutdownHook  Shutdown hook to register leader election shutdown
     */
    private static Future<Void> leaderElection(KubernetesClient client, ClusterOperatorConfig config, ShutdownHook shutdownHook)    {
        Promise<Void> leader = Promise.promise();
        Context context = Vertx.currentContext();

        if (config.getLeaderElectionConfig() != null) {
            LeaderElectionManager leaderElection = new LeaderElectionManager(
                    client, config.getLeaderElectionConfig(),
                    () -> {
                        // New leader => complete the future
                        LOGGER.info("I'm the new leader");
                        context.runOnContext(v -> leader.complete());
                    },
                    isShuttingDown -> {
                        // Not a leader anymore
                        if (!isShuttingDown) {
                            // Exit only if this isn't called as part of a shutdown
                            LOGGER.warn("Stopped being a leader => exiting");
                            // Has to run asynchronously to not block the leader election from shutting down (the exit call is synchronous)
                            CompletableFuture.runAsync(() -> System.exit(1));
                        } else {
                            LOGGER.info("Stopped being a leader during a shutdown");
                        }
                    },
                    s -> {
                        // Do nothing
                    });

            LOGGER.info("Waiting to become a leader");
            leaderElection.start();
            shutdownHook.register(leaderElection::stop);
        } else {
            LOGGER.info("Leader election is not enabled");
            leader.complete();
        }

        return leader.future();
    }

    /**
     * Start an HTTP health and metrics server
     *
     * @param vertx             Vertx instance
     * @param metricsProvider   Metrics Provider to get the metrics from
     *
     * @return Future which completes when the health and metrics webserver is started
     */
    private static Future<HttpServer> startHealthServer(Vertx vertx, MetricsProvider metricsProvider) {
        Promise<HttpServer> result = Promise.promise();

        vertx.createHttpServer()
                .requestHandler(request -> {
                    if (request.path().equals("/healthy")) {
                        request.response().setStatusCode(204).end();
                    } else if (request.path().equals("/ready")) {
                        request.response().setStatusCode(204).end();
                    } else if (request.path().equals("/metrics")) {
                        PrometheusMeterRegistry metrics = (PrometheusMeterRegistry) metricsProvider.meterRegistry();
                        request.response()
                                .putHeader("Content-Type", "text/plain; version=0.0.4; charset=utf-8")
                                .setStatusCode(200)
                                .end(metrics.scrape());
                    }
                })
                .listen(HEALTH_SERVER_PORT).onComplete(ar -> {
                    if (ar.succeeded()) {
                        LOGGER.info("Health and metrics server is ready on port {}", HEALTH_SERVER_PORT);
                    } else {
                        LOGGER.error("Failed to start health and metrics webserver on port {}", HEALTH_SERVER_PORT, ar.cause());
                    }
                    result.handle(ar);
                });

        return result.future();
    }
}
