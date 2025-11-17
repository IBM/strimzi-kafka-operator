/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.operator.cluster.operator.assembly;

import io.fabric8.kubernetes.api.model.ConfigMap;
import io.fabric8.kubernetes.api.model.HasMetadata;
import io.fabric8.kubernetes.api.model.LoadBalancerStatus;
import io.fabric8.kubernetes.api.model.OwnerReference;
import io.fabric8.kubernetes.api.model.OwnerReferenceBuilder;
import io.fabric8.kubernetes.api.model.Secret;
import io.fabric8.kubernetes.api.model.Service;
import io.fabric8.kubernetes.api.model.ServiceSpec;
import io.fabric8.kubernetes.api.model.ServiceStatus;
import io.fabric8.kubernetes.api.model.networking.v1.Ingress;
import io.fabric8.openshift.api.model.Route;
import io.strimzi.api.kafka.model.common.CertAndKeySecretSource;
import io.strimzi.api.kafka.model.kafka.KafkaResources;
import io.strimzi.api.kafka.model.kafka.listener.GenericKafkaListener;
import io.strimzi.api.kafka.model.kafka.listener.ListenerAddress;
import io.strimzi.api.kafka.model.kafka.listener.ListenerAddressBuilder;
import io.strimzi.api.kafka.model.kafka.listener.ListenerStatus;
import io.strimzi.api.kafka.model.kafka.listener.ListenerStatusBuilder;
import io.strimzi.api.kafka.model.kafka.listener.NodeAddressType;
import io.strimzi.operator.cluster.PlatformFeaturesAvailability;
import io.strimzi.operator.cluster.model.CertUtils;
import io.strimzi.operator.cluster.model.ClusterCa;
import io.strimzi.operator.cluster.model.DnsNameGenerator;
import io.strimzi.operator.cluster.model.KafkaCluster;
import io.strimzi.operator.cluster.model.ListenersUtils;
import io.strimzi.operator.cluster.model.ModelUtils;
import io.strimzi.operator.cluster.model.NodeRef;
import io.strimzi.operator.cluster.operator.resource.kubernetes.ConfigMapOperator;
import io.strimzi.operator.cluster.operator.resource.kubernetes.IngressOperator;
import io.strimzi.operator.cluster.operator.resource.kubernetes.RouteOperator;
import io.strimzi.operator.cluster.operator.resource.kubernetes.SecretOperator;
import io.strimzi.operator.cluster.operator.resource.kubernetes.ServiceOperator;
import io.strimzi.operator.cluster.stretch.RemoteResourceOperatorSupplier;
import io.strimzi.operator.cluster.stretch.spi.StretchNetworkingProvider;
import io.strimzi.operator.common.Reconciliation;
import io.strimzi.operator.common.ReconciliationLogger;
import io.strimzi.operator.common.Util;
import io.strimzi.operator.common.model.InvalidResourceException;
import io.vertx.core.Future;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static java.util.Collections.singletonList;

/**
 * Class used for reconciliation of Kafka listeners. This class contains both the steps of the Kafka
 * reconciliation pipeline related to the listeners and is also used to store the state between them.
 */
public class KafkaListenersReconciler {
    private static final ReconciliationLogger LOGGER = ReconciliationLogger.create(KafkaListenersReconciler.class.getName());

    private final Reconciliation reconciliation;
    private final long operationTimeoutMs;
    private final KafkaCluster kafka;
    private final ClusterCa clusterCa;
    private final PlatformFeaturesAvailability pfa;
    private Map<String, PlatformFeaturesAvailability> remotePfas;

    private final SecretOperator secretOperator;
    private final ServiceOperator serviceOperator;
    private final RouteOperator routeOperator;
    private final IngressOperator ingressOperator;
    private ConfigMapOperator configMapOperator;

    private Map<String, ServiceOperator> stretchServiceOperators;
    private Map<String, RouteOperator> stretchRouteOperators;
    private Map<String, IngressOperator> stretchIngressOperators;
    private Map<String, ConfigMapOperator> stretchConfigMapOperators;

    private boolean isStretchMode = false;
    private List<String> targetClusterIds;
    private String centralClusterId;

    /** Networking provider for stretch clusters. */
    private StretchNetworkingProvider networkingProvider;

    /* test */ final ReconciliationResult result;

    /**
     * Constructs the Kafka Listeners reconciler
     *
     * @param reconciliation            Reconciliation marker
     * @param kafka                     The Kafka mode
     * @param clusterCa                 The Cluster CA instance
     * @param pfa                       PlatformFeaturesAvailability describing the environment we run in
     * @param operationTimeoutMs        Timeout for Kubernetes operations
     * @param secretOperator            The Secret operator for working with Kubernetes Secrets
     * @param serviceOperator           The Service operator for working with Kubernetes Services
     * @param routeOperator             The Route operator for working with Kubernetes Route
     * @param ingressOperator           The Ingress operator for working with Kubernetes Ingress
     */
    public KafkaListenersReconciler(
            Reconciliation reconciliation,
            KafkaCluster kafka,
            ClusterCa clusterCa,
            PlatformFeaturesAvailability pfa,

            long operationTimeoutMs,

            SecretOperator secretOperator,
            ServiceOperator serviceOperator,
            RouteOperator routeOperator,
            IngressOperator ingressOperator
    ) {
        this.reconciliation = reconciliation;
        this.kafka = kafka;
        this.clusterCa = clusterCa;
        this.pfa = pfa;

        this.operationTimeoutMs = operationTimeoutMs;

        this.secretOperator = secretOperator;
        this.serviceOperator = serviceOperator;
        this.routeOperator = routeOperator;
        this.ingressOperator = ingressOperator;

        // Initialize the result object
        this.result = new ReconciliationResult();
        // Fill in the alternative names from the listener configuration
        this.result.bootstrapDnsNames.addAll(ListenersUtils.alternativeNames(kafka.getListeners()));
    }

    /**
     * Sets up attributes for stretch cluster kafka listener reconciler
     *
     * @param targetClusterIds          The Target cluster ID fot a stretched kafka clusters
     * @param centralClusterId          The central cluter ID for a stretched kafka clusters
     * @param remotePfas                Object that describes the environment remote clusters run in
     * @param configMapOperator         ConfigMap Operator for the central cluster
     * @param remoteSupplier            Remote Supplier object
     * @param networkingProvider        Networking Provider for stretch clusters
     * 
     * @return  Returns a Ka,fkaListenersReconciler instance with stretch cluster setup.
     */
    public KafkaListenersReconciler withStretchConfig(
        List<String> targetClusterIds,
        String centralClusterId,
        Map<String, PlatformFeaturesAvailability> remotePfas,
        ConfigMapOperator configMapOperator,
        RemoteResourceOperatorSupplier remoteSupplier,
        StretchNetworkingProvider networkingProvider
    ) {
        this.configMapOperator = configMapOperator;

        this.stretchServiceOperators = new HashMap<>();
        this.stretchRouteOperators = new HashMap<>();
        this.stretchIngressOperators = new HashMap<>();
        this.stretchConfigMapOperators = new HashMap<>();


        for (String clusterId : remoteSupplier.remoteResourceOperators.keySet()) {
            io.strimzi.operator.cluster.operator.resource.ResourceOperatorSupplier supplier =
                remoteSupplier.get(clusterId);

            stretchConfigMapOperators.put(clusterId, supplier.configMapOperations);
            stretchServiceOperators.put(clusterId, supplier.serviceOperations);
            stretchIngressOperators.put(clusterId, supplier.ingressOperations);
            stretchRouteOperators.put(clusterId, supplier.routeOperations);
        }

        this.isStretchMode = true;
        this.targetClusterIds = targetClusterIds;
        this.centralClusterId = centralClusterId;

        this.remotePfas = remotePfas;

        this.networkingProvider = networkingProvider;

        return this;
    }

    /**
     * The main reconciliation method which triggers the whole reconciliation pipeline. This is the method which is
     * expected to be called from the outside to trigger the reconciliation.
     *
     * @return  Future which completes when the reconciliation completes. It contains the result of the reconciliation
     *          which contains the collected addresses, prepared listener statuses etc.
     */
    public Future<ReconciliationResult> reconcile()    {
        return services()
                .compose(i -> routes())
                .compose(i -> ingresses())
                .compose(i -> internalServicesReady())
                .compose(i -> loadBalancerServicesReady())
                .compose(i -> nodePortServicesReady())
                .compose(i -> routesReady())
                .compose(i -> ingressesReady())
                .compose(i -> clusterIPServicesReady())
                .compose(i -> customListenerCertificates())
                // This method should be called only after customListenerCertificates
                .compose(customListenerCertificates -> addCertificatesToListenerStatuses(customListenerCertificates))
                .compose(i -> Future.succeededFuture(result));
    }

    /**
     * The stretch reconciliation method which triggers the whole reconciliation pipeline. 
     *
     * @return  Future which completes when the reconciliation completes. It contains the result of the reconciliation
     *          which contains the collected addresses, prepared listener statuses etc.
     */
    public Future<ReconciliationResult> stretchReconcile() {
        return stretchServices()
                .compose(i -> stretchRoutes())
                .compose(i -> stretchIngresses())
                .compose(i -> internalServicesReady())
                .compose(i -> stretchLoadBalancerServicesReady())
                .compose(i -> stretchNodePortServicesReady())
                .compose(i -> stretchRoutesReady())
                .compose(i -> stretchIngressesReady())
                .compose(i -> clusterIPServicesReady())
                .compose(i -> customListenerCertificates())
                // This method should be called only after customListenerCertificates
                .compose(customListenerCertificates -> addCertificatesToListenerStatuses(customListenerCertificates))
                .compose(i -> {
                    List<ListenerStatus> merged = mergeListenerStatuses(result.listenerStatuses);
                    result.listenerStatuses.clear();
                    result.listenerStatuses.addAll(merged);
                    return Future.succeededFuture(result);
                });
    }

    private List<ListenerStatus> mergeListenerStatuses(List<ListenerStatus> originalStatuses) {
        Map<String, ListenerStatusBuilder> mergedMap = new LinkedHashMap<>();

        for (ListenerStatus status : originalStatuses) {
            String name = status.getName();

            // Use existing builder or create new one
            ListenerStatusBuilder builder = mergedMap.computeIfAbsent(name, k -> new ListenerStatusBuilder().withName(name));

            // Get existing addresses already set in the builder
            List<ListenerAddress> existing = builder.build().getAddresses();
            List<ListenerAddress> mergedAddresses = existing != null ? new ArrayList<>(existing) : new ArrayList<>();

            // Add addresses from the current status
            if (status.getAddresses() != null) {
                mergedAddresses.addAll(status.getAddresses());
            }

            builder.withAddresses(mergedAddresses);

            // Merge certificates: take from status only if builder doesn't already have one
            if (status.getCertificates() != null && builder.build().getCertificates() == null) {
                builder.withCertificates(status.getCertificates());
            }
        }

        return mergedMap.values()
                .stream()
                .map(ListenerStatusBuilder::build)
                .collect(Collectors.toList());
    }

    /**
     * Makes sure all desired services are updated and the rest are deleted for a stretch cluster configuration. 
     * This includes the regular headless, node-port, or load balancer services.
     *
     * @return Future which completes when all services in all the clusters are created or deleted.
     */
    protected Future<Void> stretchServices() {
        List<Future<Void>> futures = new ArrayList<>();

        List<Service> commonServicesWithOwnerReference = generateDefaultServices(false);
        List<Service> commonServicesWithoutOwnerReference = generateDefaultServices(true);

        Map<String, List<Service>> clusteredPerBrokerServices = kafka.generateClusteredPerPodServices();

        for (String targetClusterId : this.targetClusterIds) {
            ServiceOperator operator = selectServiceOperator(targetClusterId);

            // Skip if operator is null (cluster not available)
            if (operator == null) {
                LOGGER.warnCr(reconciliation, "Skipping service reconciliation for remote cluster '{}' - cluster not available", targetClusterId);
                continue;
            }

            boolean isCentralCluster = targetClusterId.equals(centralClusterId);

            List<Service> commonServices = isCentralCluster
                    ? commonServicesWithOwnerReference
                    : commonServicesWithoutOwnerReference;

            List<Service> perBrokerServices = clusteredPerBrokerServices.getOrDefault(targetClusterId, Collections.emptyList());

            List<Service> services = Stream.concat(commonServices.stream(), perBrokerServices.stream())
                                        .collect(Collectors.toList());

            if (!isCentralCluster) {
                services = services.stream()
                                    .map(svc -> addGCConfigMapOwner(svc, targetClusterId))
                                    .collect(Collectors.toList());
            }

            futures.add(operator
                    .batchReconcile(
                        reconciliation,
                        reconciliation.namespace(),
                        services,
                        kafka.getSelectorLabels()
                    )
                    .mapEmpty()
            );

            LOGGER.infoCr(reconciliation, "Set GC ConfigMap as owner for {} services in remote cluster {}", services.size(), targetClusterId);
        }

        return Future.join(futures).mapEmpty();
    }

    /**
     * Generates a list of common services (service, headless service, external bootstrap services)
     * based on whether owner references should be included.
     *
     * @param withOwnerReference Whether to include owner references on the services.
     * @return List of common services.
     */
    private List<Service> generateDefaultServices(boolean withOwnerReference) {
        List<Service> services = new ArrayList<>();
        services.add(kafka.generateService(withOwnerReference));
        services.add(kafka.generateHeadlessService(withOwnerReference));
        services.addAll(kafka.generateExternalBootstrapServices(withOwnerReference));
        return services;
    }

    /**
     * Makes sure all desired services are updated and the rest is deleted. This method updates all services in one go
     *           => the regular headless, node-port or load balancer ones.
     *
     * @return  Future which completes when all services are created or deleted.
     */
    protected Future<Void> services() {
        List<Service> services = new ArrayList<>();
        services.add(kafka.generateService());
        services.add(kafka.generateHeadlessService());
        services.addAll(kafka.generateExternalBootstrapServices());
        services.addAll(kafka.generatePerPodServices());

        return serviceOperator.batchReconcile(reconciliation, reconciliation.namespace(), services, kafka.getSelectorLabels()).mapEmpty();
    }

    /**
     * Makes sure all desired routes are updated and the rest is deleted.
     *
     * @return Future which completes when all routes are created or deleted.
     */
    protected Future<Void> routes() {
        List<Route> routes = new ArrayList<>(kafka.generateExternalBootstrapRoutes(false));
        routes.addAll(kafka.generateExternalRoutes(null));

        if (pfa.hasRoutes()) {
            return routeOperator.batchReconcile(reconciliation, reconciliation.namespace(), routes, kafka.getSelectorLabels()).mapEmpty();
        } else {
            if (!routes.isEmpty()) {
                LOGGER.warnCr(reconciliation, "The OpenShift route API is not available in this Kubernetes cluster. Exposing Kafka cluster {} using routes is not possible.", reconciliation.name());
                return Future.failedFuture("The OpenShift route API is not available in this Kubernetes cluster. Exposing Kafka cluster " + reconciliation.name() + " using routes is not possible.");
            } else {
                return Future.succeededFuture();
            }
        }
    }

    /**
     * Makes sure all desired routes in the central and remote clusters are updated and the rest is deleted.
     *
     * @return Future which completes when all routes are created or deleted.
     */
    protected Future<Void> stretchRoutes() {
        boolean hasPfaOnEveryCluster = remotePfas.entrySet().stream().allMatch(x -> x.getValue().hasRoutes());
       

        List<Future<Void>> futures = new ArrayList<>();

        List<Route> bootstrapRoutesWithOwnerReferences = kafka.generateExternalBootstrapRoutes(false);
        List<Route> bootstrapRoutesWithoutOwnerReferences = kafka.generateExternalBootstrapRoutes(true);

        Map<String, List<Route>> clusteredRoutes = kafka.generateClusteredExternalRoutes();

        if (!hasPfaOnEveryCluster) {
            if (!bootstrapRoutesWithOwnerReferences.isEmpty()) {
                LOGGER.warnCr(reconciliation, "The OpenShift route API is not available in this Kubernetes cluster. Exposing Kafka cluster {} using routes is not possible.", reconciliation.name());
                return Future.failedFuture("The OpenShift route API is not available in this Kubernetes cluster. Exposing Kafka cluster " + reconciliation.name() + " using routes is not possible.");
            } else {
                return Future.succeededFuture();
            }
        }

        for (String targetClusterId : this.targetClusterIds) {
            RouteOperator operator = selectRouteOperator(targetClusterId);

            // Skip if operator is null (cluster not available)
            if (operator == null) {
                LOGGER.warnCr(reconciliation, "Skipping route reconciliation for remote cluster '{}' - cluster not available", targetClusterId);
                continue;
            }

            boolean isCentralCluster = targetClusterId.equals(centralClusterId);
            List<Route> routes = Stream.concat(
                                                    (isCentralCluster ? 
                                                        bootstrapRoutesWithOwnerReferences : 
                                                        bootstrapRoutesWithoutOwnerReferences
                                                    ).stream(), 
                                                    clusteredRoutes.get(targetClusterId).stream()
                                                )
                                            .collect(Collectors.toList());

            if (!isCentralCluster) {
                routes = routes.stream()
                                .map(route -> addGCConfigMapOwner(route, targetClusterId))
                                .collect(Collectors.toList());
            }
            futures.add(
                operator
                .batchReconcile(
                    reconciliation, 
                    reconciliation.namespace(), 
                    routes, 
                    kafka.getSelectorLabels()
                ).mapEmpty());

            LOGGER.infoCr(reconciliation,
                "Set GC ConfigMap as owner for {} routes in remote cluster {}",
                routes.size(), targetClusterId);
        }

        return Future.join(futures).mapEmpty();
    }

    /**
     * Makes sure all desired ingresses are updated and the rest is deleted.
     *
     * @return  Future which completes when all ingresses are created or deleted.
     */
    protected Future<Void> ingresses() {
        List<Ingress> ingresses = new ArrayList<>(kafka.generateExternalBootstrapIngresses());
        ingresses.addAll(kafka.generateExternalIngresses(null));

        return ingressOperator.batchReconcile(reconciliation, reconciliation.namespace(), ingresses, kafka.getSelectorLabels()).mapEmpty();
    }

    /**
     * Reconciles the desired Ingress resources in both central and remote clusters by ensuring all required Ingresses
     * are created or updated, and any obsolete ones are deleted.
     *
     * @return Future that completes when all Ingress resources have been reconciled across clusters.
     */
    protected Future<Void> stretchIngresses() {
        List<Future<Void>> futures = new ArrayList<>();

        List<Ingress> bootstrapIngressesWithOwnerReferences = kafka.generateExternalBootstrapIngresses(false);
        List<Ingress> bootstrapIngressesWithoutOwnerReferences = kafka.generateExternalBootstrapIngresses(true);

        Map<String, List<Ingress>> clusteredIngresses = kafka.generateClusteredExternalIngresses();

        for (String targetClusterId : this.targetClusterIds) {
            boolean isCentralCluster = targetClusterId.equals(centralClusterId);

            IngressOperator operator = isCentralCluster
                    ? ingressOperator
                    : stretchIngressOperators.get(targetClusterId);

            // Skip if operator is null (cluster not available)
            if (operator == null) {
                LOGGER.warnCr(reconciliation, "Skipping ingress reconciliation for remote cluster '{}' - cluster not available", targetClusterId);
                continue;
            }

            List<Ingress> bootstrapIngresses = isCentralCluster
                    ? bootstrapIngressesWithOwnerReferences
                    : bootstrapIngressesWithoutOwnerReferences;

            List<Ingress> perBrokerIngresses = clusteredIngresses.getOrDefault(targetClusterId, Collections.emptyList());

            List<Ingress> ingresses = Stream.concat(bootstrapIngresses.stream(), perBrokerIngresses.stream())
                                            .collect(Collectors.toList());

            if (!isCentralCluster) {
                ingresses = ingresses.stream()
                                    .map(ingress -> addGCConfigMapOwner(ingress, targetClusterId))
                                    .collect(Collectors.toList());
            }

            futures.add(
                operator.batchReconcile(
                    reconciliation,
                    reconciliation.namespace(),
                    ingresses,
                    kafka.getSelectorLabels()
                ).mapEmpty()
            );

            LOGGER.infoCr(reconciliation,
                    "Set GC ConfigMap as owner for {} ingresses in remote cluster {}",
                    ingresses.size(), targetClusterId);
        }

        return Future.join(futures).mapEmpty();
    }
    

    /**
     * Utility method which helps to register the advertised hostnames for a specific listener of a specific broker.
     * It decides between the user-customized advertised hostname and the actual broker address. If both are not null,
     * the customized advertised hostname is used.
     *
     * @param nodeId                ID of the broker to which this hostname belongs
     * @param listener              The Listener for which is this hostname used
     * @param advertisedHostname    User customized advertised hostname
     * @param brokerHostname        The hostname which might be used for the broker when no overrides are configured
     */
    private void registerAdvertisedHostname(int nodeId, GenericKafkaListener listener, String advertisedHostname, String brokerHostname)   {
        result.advertisedHostnames
                .computeIfAbsent(nodeId, id -> new HashMap<>())
                .put(ListenersUtils.envVarIdentifier(listener), advertisedHostname != null ? advertisedHostname : brokerHostname);
    }

    /**
     * Utility method which helps to register the advertised port for a specific listener of a specific broker.
     * The broker port passed to this method is based on the infrastructure (Service, Load Balancer, etc.).
     * This method in addition checks for any overrides and uses them if configured.
     *
     * @param brokerId      ID of the broker to which this port belongs
     * @param listener      The Listener for which is this port used
     * @param brokerPort    The port which might be used for the broker when no overrides are configured
     */
    private void registerAdvertisedPort(int brokerId, GenericKafkaListener listener, int brokerPort)   {
        result.advertisedPorts
                .computeIfAbsent(brokerId, id -> new HashMap<>())
                .put(ListenersUtils.envVarIdentifier(listener), ListenersUtils.advertisedPortFromOverrideOrParameter(listener, brokerId, brokerPort));
    }

    /**
     * Generates the name of the environment variable which will contain the advertised address for given listener. The
     * environment variable will be different for Node Port listeners which need to consume the address from the init
     * container corresponding to their preferred node.
     *
     * @param listener              The listener
     * @return                      The environment variable which will have the address
     */
    private static String nodePortAddressEnvVar(GenericKafkaListener listener)  {
        String preferredNodeAddressType;
        NodeAddressType preferredType = ListenersUtils.preferredNodeAddressType(listener);

        if (preferredType != null && preferredType.toValue() != null)  {
            preferredNodeAddressType = preferredType.toValue().toLowerCase(Locale.ENGLISH);
        } else {
            preferredNodeAddressType = "default";
        }

        return String.format("${strimzifile:/opt/kafka/init/external.address:nodeport.%s.address}", preferredNodeAddressType);
    }

    /**
     * Generates the hostname of an internal service with or without the DNS suffix
     *
     * @param namespace             Namespace of the service
     * @param serviceName           Name of the service
     * @param useServiceDnsDomain   Flag indicating whether the address should contain the DNS suffix or not
     *
     * @return  The DNS name of the service
     */
    private static String getInternalServiceHostname(String namespace, String serviceName, boolean useServiceDnsDomain)    {
        if (useServiceDnsDomain)    {
            return DnsNameGenerator.serviceDnsNameWithClusterDomain(namespace, serviceName);
        } else {
            return DnsNameGenerator.serviceDnsNameWithoutClusterDomain(namespace, serviceName);
        }
    }

    /**
     * Generates the hostname of an internal service with or without the DNS suffix
     * and with the cluster ID
     *
     * @param clusterId             The cluster Id of the internal service
     * @param namespace             Namespace of the service
     * @param serviceName           Name of the service
     * @param useServiceDnsDomain   Flag indicating whether the address should contain the DNS suffix or not
     *
     * @return  The DNS name of the service
     */
    private static String getInternalServiceHostnameWithClusterId(String clusterId, String namespace, String serviceName, boolean useServiceDnsDomain)    {
        return DnsNameGenerator.serviceDnsNameWithClusterDomainAndClusterId(clusterId, namespace, serviceName);
    }

    /**
     * Checks the readiness of the internal services. The internal services are ready out of the box and there is no
     * need to wait for them. But this method at least collects their addresses for the reconciliation result and
     * prepares the listener statuses.
     *
     * @return  Future which completes when the internal services are ready and their addresses are collected
     */
    protected Future<Void> internalServicesReady()   {
        for (GenericKafkaListener listener : ListenersUtils.internalListeners(kafka.getListeners())) {
            boolean useServiceDnsDomain = (listener.getConfiguration() != null && listener.getConfiguration().getUseServiceDnsDomain() != null)
                    ? listener.getConfiguration().getUseServiceDnsDomain() : false;

            // Set status based on bootstrap service
            String bootstrapAddress = getInternalServiceHostname(reconciliation.namespace(), ListenersUtils.backwardsCompatibleBootstrapServiceName(reconciliation.name(), listener), useServiceDnsDomain);

            ListenerStatus ls = new ListenerStatusBuilder()
                    .withName(listener.getName())
                    .withAddresses(new ListenerAddressBuilder()
                            .withHost(bootstrapAddress)
                            .withPort(listener.getPort())
                            .build())
                    .build();
            result.listenerStatuses.add(ls);

            // Set advertised hostnames and ports
            for (NodeRef node : kafka.brokerNodes()) {
                String brokerAddress;
                
                if (isStretchMode) {
                    brokerAddress = networkingProvider.generatePodDnsName(
                        reconciliation.namespace(),
                        KafkaResources.brokersServiceName(reconciliation.name()),
                        node.podName(),
                        node.clusterId()
                    );
                } else if (useServiceDnsDomain) {
                    brokerAddress = DnsNameGenerator.podDnsNameWithClusterDomain(reconciliation.namespace(), KafkaResources.brokersServiceName(reconciliation.name()), node.podName());
                } else {
                    brokerAddress = DnsNameGenerator.podDnsNameWithoutClusterDomain(reconciliation.namespace(), KafkaResources.brokersServiceName(reconciliation.name()), node.podName());
                }

                String userConfiguredAdvertisedHostname = ListenersUtils.brokerAdvertisedHost(listener, node);
                if (userConfiguredAdvertisedHostname != null && listener.isTls()) {
                    // If user configured a custom advertised hostname, add it to the SAN names used in the certificate
                    result.brokerDnsNames.computeIfAbsent(node.nodeId(), k -> new HashSet<>(1)).add(userConfiguredAdvertisedHostname);
                }

                registerAdvertisedHostname(node.nodeId(), listener, userConfiguredAdvertisedHostname, brokerAddress);
                registerAdvertisedPort(node.nodeId(), listener, listener.getPort());
            }
        }

        return Future.succeededFuture();
    }

    /**
     * Collect all addresses of services related to clusterIP addresses for Statuses, certificates and advertised
     * addresses. This method for all ClusterIP type listeners:
     *      1) Collects the relevant addresses of bootstrap service and stores them for use in certificates
     *      2) Collects the clusterIP addresses for certificates and advertised hostnames
     *
     * @return  Future which completes clusterIP service addresses are collected
     */
    protected Future<Void> clusterIPServicesReady()   {
        for (GenericKafkaListener listener : ListenersUtils.clusterIPListeners(kafka.getListeners())) {
            boolean useServiceDnsDomain = (listener.getConfiguration() != null && listener.getConfiguration().getUseServiceDnsDomain() != null)
                    ? listener.getConfiguration().getUseServiceDnsDomain() : false;

            String bootstrapAddress = getInternalServiceHostname(reconciliation.namespace(), ListenersUtils.backwardsCompatibleBootstrapServiceName(reconciliation.name(), listener), useServiceDnsDomain);

            if (listener.isTls()) {
                result.bootstrapDnsNames.addAll(ModelUtils.generateAllServiceDnsNames(reconciliation.namespace(), ListenersUtils.backwardsCompatibleBootstrapServiceName(reconciliation.name(), listener)));
            }

            ListenerStatus ls = new ListenerStatusBuilder()
                    .withName(listener.getName())
                    .withAddresses(new ListenerAddressBuilder()
                            .withHost(bootstrapAddress)
                            .withPort(listener.getPort())
                            .build())
                    .build();
            result.listenerStatuses.add(ls);

            // Set advertised hostnames and ports
            for (NodeRef node : kafka.brokerNodes()) {
                String brokerServiceName = ListenersUtils.backwardsCompatiblePerBrokerServiceName(ReconcilerUtils.getControllerNameFromPodName(node.podName()), node.nodeId(), listener);

                String brokerAddress;

                if (isStretchMode) {
                    brokerAddress = getInternalServiceHostnameWithClusterId(
                        node.clusterId(), 
                        reconciliation.namespace(), 
                        brokerServiceName, 
                        useServiceDnsDomain
                    );
                } else {
                    brokerAddress = getInternalServiceHostname(
                        reconciliation.namespace(), 
                        brokerServiceName, 
                        useServiceDnsDomain
                    );
                }
                
                String userConfiguredAdvertisedHostname = ListenersUtils.brokerAdvertisedHost(listener, node);

                if (listener.isTls()) {
                    result.brokerDnsNames.computeIfAbsent(node.nodeId(), k -> new HashSet<>(2)).add(brokerAddress);

                    if (userConfiguredAdvertisedHostname != null) {
                        result.brokerDnsNames.get(node.nodeId()).add(userConfiguredAdvertisedHostname);
                    }
                }

                registerAdvertisedHostname(node.nodeId(), listener, userConfiguredAdvertisedHostname, brokerAddress);
                registerAdvertisedPort(node.nodeId(), listener, listener.getPort());
            }
        }

        return Future.succeededFuture();
    }

    /**
     * Ensures all load balancer services across clusters are provisioned and ready.
     * For each load balancer-type listener:
     * <ol>
     *   <li>Checks if the bootstrap service has been provisioned and has an ingress address</li>
     *   <li>Collects addresses for certificates and Kafka CR status</li>
     *   <li>Checks if broker services are provisioned and collect their addresses</li>
     *   <li>Stores broker addresses for use in advertised listeners and certificates</li>
     * </ol>
     *
     * @return Future which completes when all load balancer services are ready and addresses are collected.
     */
    protected Future<Void> stretchLoadBalancerServicesReady() {
        List<GenericKafkaListener> loadBalancerListeners = ListenersUtils.loadBalancerListeners(kafka.getListeners());
        List<Future<Void>> listenerFutures = new ArrayList<>(loadBalancerListeners.size());

        for (GenericKafkaListener listener : loadBalancerListeners) {
            String bootstrapServiceName = ListenersUtils.backwardsCompatibleBootstrapServiceName(reconciliation.name(), listener);
            List<String> bootstrapListenerAddressList = new ArrayList<>();

            Future<Void> perListenerFuture = Future.succeededFuture()
                .compose(ignore -> {
                    if (ListenersUtils.skipCreateBootstrapService(listener)) {
                        return Future.succeededFuture();
                    }

                    List<Future<?>> addressFutures = new ArrayList<>();
                    for (String clusterId : targetClusterIds) {
                        addressFutures.add(selectServiceOperator(clusterId).hasIngressAddress(
                            reconciliation,
                            reconciliation.namespace(),
                            bootstrapServiceName,
                            1_000,
                            operationTimeoutMs
                        ));
                    }

                    return Future.join(addressFutures).compose(joined -> {
                        List<Future<Service>> bootstrapServices = targetClusterIds.stream()
                            .map(clusterId -> selectServiceOperator(clusterId).getAsync(reconciliation.namespace(), bootstrapServiceName))
                            .collect(Collectors.toList());
                        return Future.join(bootstrapServices);
                    }).compose(servicesResult -> {
                        for (Object svcObj : servicesResult.result().list()) {
                            Service svc = (Service) svcObj;
                            String bootstrapAddress = extractLoadBalancerAddress(svc);
                            if (bootstrapAddress != null) {
                                LOGGER.debugCr(reconciliation, "Found address {} for Service {}", bootstrapAddress, bootstrapServiceName);
                                result.bootstrapDnsNames.add(bootstrapAddress);
                                bootstrapListenerAddressList.add(bootstrapAddress);
                            }
                        }
                        return Future.succeededFuture();
                    });
                })
                .compose(res -> {
                    List<Future<Void>> perPodFutures = kafka.brokerNodes().stream()
                        .map(node -> selectServiceOperator(node.clusterId()).hasIngressAddress(
                            reconciliation,
                            reconciliation.namespace(),
                            ListenersUtils.backwardsCompatiblePerBrokerServiceName(ReconcilerUtils.getControllerNameFromPodName(node.podName()), node.nodeId(), listener),
                            1_000,
                            operationTimeoutMs))
                        .collect(Collectors.toList());
                    return Future.join(perPodFutures);
                })
                .compose(ignore -> {
                    List<Future<Object>> perPodFutures = kafka.brokerNodes().stream()
                        .map(node -> selectServiceOperator(node.clusterId())
                            .getAsync(reconciliation.namespace(), ListenersUtils.backwardsCompatiblePerBrokerServiceName(ReconcilerUtils.getControllerNameFromPodName(node.podName()), node.nodeId(), listener))
                            .compose(service -> {
                                Service svc = (Service) service;
                                String brokerAddress = extractLoadBalancerAddress(svc);

                                if (brokerAddress != null) {
                                    LOGGER.debugCr(reconciliation, "Found address {} for Service {}", brokerAddress, svc.getMetadata().getName());
                                    if (ListenersUtils.skipCreateBootstrapService(listener)) {
                                        bootstrapListenerAddressList.add(brokerAddress);
                                    }
                                    result.brokerDnsNames.computeIfAbsent(node.nodeId(), k -> new HashSet<>(2)).add(brokerAddress);

                                    String advertisedHostname = ListenersUtils.brokerAdvertisedHost(listener, node);
                                    if (advertisedHostname != null) {
                                        result.brokerDnsNames.get(node.nodeId()).add(advertisedHostname);
                                    }

                                    registerAdvertisedHostname(node.nodeId(), listener, advertisedHostname, brokerAddress);
                                    registerAdvertisedPort(node.nodeId(), listener, listener.getPort());
                                }

                                return Future.succeededFuture().mapEmpty();
                            }))
                        .collect(Collectors.toList());
                    return Future.join(perPodFutures);
                })
                .compose(res -> {
                    ListenerStatus ls = new ListenerStatusBuilder()
                        .withName(listener.getName())
                        .withAddresses(bootstrapListenerAddressList.stream()
                            .map(address -> new ListenerAddressBuilder()
                                .withHost(address)
                                .withPort(listener.getPort())
                                .build())
                            .collect(Collectors.toList()))
                        .build();
                    result.listenerStatuses.add(ls);
                    return Future.succeededFuture();
                });

            listenerFutures.add(perListenerFuture);
        }

        return Future.join(listenerFutures).mapEmpty();
    }

    /**
     * Extracts the hostname or IP address from the first ingress entry of the service's LoadBalancer status.
     *
     * @param svc The Kubernetes Service resource.
     * @return The hostname if present, otherwise the IP address; or null if no ingress is available.
     */
    private String extractLoadBalancerAddress(Service svc) {
        return Optional.ofNullable(svc.getStatus())
                .map(ServiceStatus::getLoadBalancer)
                .map(LoadBalancerStatus::getIngress)
                .filter(ingresses -> !ingresses.isEmpty())
                .map(ingresses -> ingresses.get(0))
                .map(ingress ->
                    ingress.getHostname() != null ? ingress.getHostname() : ingress.getIp()
                )
                .orElse(null);
    }

    private ServiceOperator selectServiceOperator(String clusterId) {
        return clusterId.equals(centralClusterId)
            ? serviceOperator
            : stretchServiceOperators.get(clusterId);
    }


    /**
     * Makes sure all services related to load balancers are ready and collects their addresses for Statuses,
     * certificates and advertised addresses. This method for all Load Balancer type listeners:
     *      1) Checks if the bootstrap service has been provisioned (has a loadbalancer address)
     *      2) Collects the relevant addresses and stores them for use in certificates and in CR status
     *      3) Checks if the broker services have been provisioned (have a loadbalancer address)
     *      4) Collects the loadbalancer addresses for certificates and advertised hostnames
     *
     * @return  Future which completes when all Load Balancer services are ready and their addresses are collected
     */
    protected Future<Void> loadBalancerServicesReady() {
        List<GenericKafkaListener> loadBalancerListeners = ListenersUtils.loadBalancerListeners(kafka.getListeners());
        List<Future<Void>> listenerFutures = new ArrayList<>(loadBalancerListeners.size());

        for (GenericKafkaListener listener : loadBalancerListeners) {
            String bootstrapServiceName = ListenersUtils.backwardsCompatibleBootstrapServiceName(reconciliation.name(), listener);

            List<String> bootstrapListenerAddressList = new ArrayList<>();

            Future<Void> perListenerFut = Future.succeededFuture().compose(i -> {
                if (ListenersUtils.skipCreateBootstrapService(listener)) {
                    return Future.succeededFuture();
                } else {
                    return serviceOperator.hasIngressAddress(reconciliation, reconciliation.namespace(), bootstrapServiceName, 1_000, operationTimeoutMs)
                            .compose(res -> serviceOperator.getAsync(reconciliation.namespace(), bootstrapServiceName))
                            .compose(svc -> {
                                String bootstrapAddress;

                                if (svc.getStatus().getLoadBalancer().getIngress().get(0).getHostname() != null) {
                                    bootstrapAddress = svc.getStatus().getLoadBalancer().getIngress().get(0).getHostname();
                                } else {
                                    bootstrapAddress = svc.getStatus().getLoadBalancer().getIngress().get(0).getIp();
                                }

                                LOGGER.debugCr(reconciliation, "Found address {} for Service {}", bootstrapAddress, bootstrapServiceName);

                                result.bootstrapDnsNames.add(bootstrapAddress);
                                bootstrapListenerAddressList.add(bootstrapAddress);
                                return Future.succeededFuture();
                            });
                }
            }).compose(res -> {
                List<Future<Void>> perPodFutures = new ArrayList<>();

                for (NodeRef node : kafka.brokerNodes()) {
                    perPodFutures.add(
                            serviceOperator.hasIngressAddress(reconciliation, reconciliation.namespace(), ListenersUtils.backwardsCompatiblePerBrokerServiceName(ReconcilerUtils.getControllerNameFromPodName(node.podName()), node.nodeId(), listener), 1_000, operationTimeoutMs)
                    );
                }

                return Future.join(perPodFutures);
            }).compose(res -> {
                List<Future<Void>> perPodFutures = new ArrayList<>();

                for (NodeRef node : kafka.brokerNodes()) {
                    Future<Void> perBrokerFut = serviceOperator.getAsync(reconciliation.namespace(), ListenersUtils.backwardsCompatiblePerBrokerServiceName(ReconcilerUtils.getControllerNameFromPodName(node.podName()), node.nodeId(), listener))
                            .compose(svc -> {
                                String brokerAddress;

                                if (svc.getStatus().getLoadBalancer().getIngress().get(0).getHostname() != null)    {
                                    brokerAddress = svc.getStatus().getLoadBalancer().getIngress().get(0).getHostname();
                                } else {
                                    brokerAddress = svc.getStatus().getLoadBalancer().getIngress().get(0).getIp();
                                }
                                LOGGER.debugCr(reconciliation, "Found address {} for Service {}", brokerAddress, svc.getMetadata().getName());

                                if (ListenersUtils.skipCreateBootstrapService(listener)) {
                                    bootstrapListenerAddressList.add(brokerAddress);
                                }
                                result.brokerDnsNames.computeIfAbsent(node.nodeId(), k -> new HashSet<>(2)).add(brokerAddress);

                                String advertisedHostname = ListenersUtils.brokerAdvertisedHost(listener, node);
                                if (advertisedHostname != null) {
                                    result.brokerDnsNames.get(node.nodeId()).add(advertisedHostname);
                                }

                                registerAdvertisedHostname(node.nodeId(), listener, advertisedHostname, brokerAddress);
                                registerAdvertisedPort(node.nodeId(), listener, listener.getPort());

                                return Future.succeededFuture();
                            });

                    perPodFutures.add(perBrokerFut);
                }

                return Future.join(perPodFutures);
            }).compose(res -> {
                ListenerStatus ls = new ListenerStatusBuilder()
                        .withName(listener.getName())
                        .withAddresses(bootstrapListenerAddressList.stream()
                                .map(listenerAddress -> new ListenerAddressBuilder().withHost(listenerAddress)
                                        .withPort(listener.getPort())
                                        .build())
                                .collect(Collectors.toList()))
                        .build();
                result.listenerStatuses.add(ls);

                return Future.succeededFuture();
            });

            listenerFutures.add(perListenerFut);
        }

        return Future
                .join(listenerFutures)
                .mapEmpty();
    }

    /**
     * Ensures all NodePort services are ready in all clusters and collects their ports for status,
     * certificates, and advertised addresses.
     *
     * @return Future completing when all NodePort services are ready and ports are collected.
     */
    protected Future<Void> stretchNodePortServicesReady() {
        List<GenericKafkaListener> nodePortListeners = ListenersUtils.nodePortListeners(kafka.getListeners());
        List<Future<?>> listenerFutures = new ArrayList<>(nodePortListeners.size());

        for (GenericKafkaListener listener : nodePortListeners) {
            String bootstrapServiceName = ListenersUtils.backwardsCompatibleBootstrapServiceName(reconciliation.name(), listener);

            // Step 1: Wait until all bootstrap NodePort services are provisioned
            List<Future<?>> readinessChecks = targetClusterIds.stream()
                .map(clusterId -> selectServiceOperator(clusterId)
                    .hasNodePort(reconciliation, reconciliation.namespace(), bootstrapServiceName, 1_000, operationTimeoutMs))
                .collect(Collectors.toList());

            Future listenerFuture = Future.join(readinessChecks)
                // Step 2: Fetch bootstrap NodePort services and collect ports
                .compose(ignore -> {
                    List<Future<Service>> bootstrapServices = targetClusterIds.stream()
                        .map(clusterId -> selectServiceOperator(clusterId)
                            .getAsync(reconciliation.namespace(), bootstrapServiceName))
                        .collect(Collectors.toList());

                    return Future.join(bootstrapServices);
                })
                .compose(servicesFuture -> {
                    servicesFuture.result().list().forEach(service -> {
                        Integer nodePort = extractNodePort((Service) service);
                        if (nodePort != null) {
                            LOGGER.debugCr(reconciliation, "Found node port {} for Service {}", nodePort, bootstrapServiceName);
                            result.bootstrapNodePorts.put(ListenersUtils.identifier(listener), nodePort);
                        }
                    });
                    return Future.succeededFuture();
                })
                // Step 3: Collect broker node ports and advertised hostnames
                .compose(ignore -> {
                    List<Future<Object>> brokerFutures = kafka.brokerNodes().stream()
                        .map(node -> {
                            String serviceName = ListenersUtils.backwardsCompatiblePerBrokerServiceName(
                                ReconcilerUtils.getControllerNameFromPodName(node.podName()), node.nodeId(), listener
                            );

                            return selectServiceOperator(node.clusterId())
                                .getAsync(reconciliation.namespace(), serviceName)
                                .compose(svc -> {
                                    Integer nodePort = extractNodePort(svc);
                                    
                                    String advertisedHostname = ListenersUtils.brokerAdvertisedHost(listener, node);

                                    if (nodePort != null) {
                                        LOGGER.debugCr(reconciliation, "Found node port {} for Service {}", nodePort, svc.getMetadata().getName());
                                        
                                        if (advertisedHostname != null) {
                                            result.brokerDnsNames.computeIfAbsent(node.nodeId(), k -> new HashSet<>(1)).add(advertisedHostname);
                                        }

                                        registerAdvertisedPort(node.nodeId(), listener, nodePort);
                                        registerAdvertisedHostname(node.nodeId(), listener,
                                            advertisedHostname,
                                            nodePortAddressEnvVar(listener));
                                    }
                                    return Future.succeededFuture();
                                });
                        }).collect(Collectors.toList());

                    return Future.join(brokerFutures);
                }).compose(res -> {
                    ListenerStatus ls = new ListenerStatusBuilder()
                            .withName(listener.getName())
                            .build();
                    result.listenerStatuses.add(ls);

                    return Future.succeededFuture();
                });

            listenerFutures.add(listenerFuture);
        }

        return Future.join(listenerFutures).mapEmpty();
    }

    /**
     * Utility to extract the first NodePort from a Service, null-safe.
     */
    private Integer extractNodePort(Service svc) {
        return Optional.ofNullable(svc)
                .map(Service::getSpec)
                .map(ServiceSpec::getPorts)
                .filter(ports -> !ports.isEmpty())
                .map(ports -> ports.get(0).getNodePort())
                .orElse(null);
    }


    /**
     * Makes sure all services related to node ports are ready and collects their addresses for Statuses,
     * certificates and advertised addresses. This method for all NodePort type listeners:
     *      1) Checks if the bootstrap service has been provisioned (has a node port)
     *      2) Collects the node port for use in CR status
     *      3) Checks it the broker services have been provisioned (have a node port)
     *      4) Collects the node ports for advertised hostnames
     *
     * @return  Future which completes when all Node Port services are ready and their ports are collected
     */
    protected Future<Void> nodePortServicesReady() {
        List<GenericKafkaListener> loadBalancerListeners = ListenersUtils.nodePortListeners(kafka.getListeners());
        List<Future<?>> listenerFutures = new ArrayList<>(loadBalancerListeners.size());

        for (GenericKafkaListener listener : loadBalancerListeners) {
            String bootstrapServiceName = ListenersUtils.backwardsCompatibleBootstrapServiceName(reconciliation.name(), listener);

            @SuppressWarnings({ "rawtypes" }) // Has to use Raw type because of the CompositeFuture
            Future perListenerFut = serviceOperator.hasNodePort(reconciliation, reconciliation.namespace(), bootstrapServiceName, 1_000, operationTimeoutMs)
                    .compose(res -> serviceOperator.getAsync(reconciliation.namespace(), bootstrapServiceName))
                    .compose(svc -> {
                        Integer externalBootstrapNodePort = svc.getSpec().getPorts().get(0).getNodePort();
                        LOGGER.debugCr(reconciliation, "Found node port {} for Service {}", externalBootstrapNodePort, bootstrapServiceName);
                        result.bootstrapNodePorts.put(ListenersUtils.identifier(listener), externalBootstrapNodePort);

                        return Future.succeededFuture();
                    })
                    .compose(res -> {
                        List<Future<Void>> perPodFutures = new ArrayList<>();

                        for (NodeRef node : kafka.brokerNodes()) {
                            perPodFutures.add(
                                    serviceOperator.hasNodePort(reconciliation, reconciliation.namespace(), ListenersUtils.backwardsCompatiblePerBrokerServiceName(ReconcilerUtils.getControllerNameFromPodName(node.podName()), node.nodeId(), listener), 1_000, operationTimeoutMs)
                            );
                        }

                        return Future.join(perPodFutures);
                    })
                    .compose(res -> {
                        List<Future<Void>> perPodFutures = new ArrayList<>();

                        for (NodeRef node : kafka.brokerNodes()) {
                            Future<Void> perBrokerFut = serviceOperator.getAsync(reconciliation.namespace(), ListenersUtils.backwardsCompatiblePerBrokerServiceName(ReconcilerUtils.getControllerNameFromPodName(node.podName()), node.nodeId(), listener))
                                    .compose(svc -> {
                                        Integer externalBrokerNodePort = svc.getSpec().getPorts().get(0).getNodePort();
                                        LOGGER.debugCr(reconciliation, "Found node port {} for Service {}", externalBrokerNodePort, svc.getMetadata().getName());

                                        registerAdvertisedPort(node.nodeId(), listener, externalBrokerNodePort);

                                        String advertisedHostname = ListenersUtils.brokerAdvertisedHost(listener, node);

                                        if (advertisedHostname != null) {
                                            result.brokerDnsNames.computeIfAbsent(node.nodeId(), k -> new HashSet<>(1)).add(advertisedHostname);
                                        }

                                        registerAdvertisedHostname(node.nodeId(), listener, advertisedHostname, nodePortAddressEnvVar(listener));

                                        return Future.succeededFuture();
                                    });

                            perPodFutures.add(perBrokerFut);
                        }

                        return Future.join(perPodFutures);
                    }).compose(res -> {
                        ListenerStatus ls = new ListenerStatusBuilder()
                                .withName(listener.getName())
                                .build();
                        result.listenerStatuses.add(ls);

                        return Future.succeededFuture();
                    });

            listenerFutures.add(perListenerFut);
        }

        return Future
                .join(listenerFutures)
                .mapEmpty();
    }

    /**
     * Ensures all Routes are ready in all clusters and collects their addresses for Statuses,
     * certificates, and advertised addresses.
     *
     * @return Future completing when all Routes are ready and addresses are collected.
     */
    protected Future<Void> stretchRoutesReady() {
        List<GenericKafkaListener> routeListeners = ListenersUtils.routeListeners(kafka.getListeners());
        List<Future<?>> listenerFutures = new ArrayList<>(routeListeners.size());

        for (GenericKafkaListener listener : routeListeners) {
            String bootstrapRouteName = ListenersUtils.backwardsCompatibleBootstrapRouteOrIngressName(reconciliation.name(), listener);

            // Step 1: Wait until all bootstrap routes are ready
            List<Future<?>> bootstrapRouteReadinessFutures = targetClusterIds.stream()
                .map(clusterId -> {
                    RouteOperator operator = selectRouteOperator(clusterId);
                    if (operator == null) {
                        LOGGER.warnCr(reconciliation, "Skipping route readiness check for cluster '{}' - cluster not available", clusterId);
                        return Future.succeededFuture();
                    }
                    return operator.hasAddress(reconciliation, reconciliation.namespace(), bootstrapRouteName, 1_000, operationTimeoutMs);
                })
                .collect(Collectors.toList());

            Future perListenerFuture = Future.join(bootstrapRouteReadinessFutures)
                // Step 2: Collect bootstrap addresses
                .compose(ignore -> {
                    List<Future<Route>> bootstrapRoutes = targetClusterIds.stream()
                        .map(clusterId -> {
                            RouteOperator operator = selectRouteOperator(clusterId);
                            if (operator == null) {
                                LOGGER.warnCr(reconciliation, "Skipping bootstrap route collection for cluster '{}' - cluster not available", clusterId);
                                return Future.succeededFuture((Route) null);
                            }
                            return operator.getAsync(reconciliation.namespace(), bootstrapRouteName);
                        })
                        .collect(Collectors.toList());
                    return Future.join(bootstrapRoutes);
                })
                .compose(routesFuture -> {
                    List<Route> routes = routesFuture.result().list();
                    for (Route route : routes) {
                        if (route != null) {
                            String host = extractRouteHost(route);
                            if (host != null) {
                                LOGGER.debugCr(reconciliation, "Found address {} for Route {}", host, bootstrapRouteName);

                                result.bootstrapDnsNames.add(host);

                                ListenerStatus listenerStatus = new ListenerStatusBuilder()
                                    .withName(listener.getName())
                                    .withAddresses(new ListenerAddressBuilder()
                                        .withHost(host)
                                        .withPort(KafkaCluster.ROUTE_PORT)
                                        .build())
                                    .build();
                                result.listenerStatuses.add(listenerStatus);
                            }
                        }
                    }
                    return Future.succeededFuture();
                })
                // Step 3: Per-broker routes
                .compose(ignore -> {
                    List<Future<Object>> brokerFutures = kafka.brokerNodes().stream()
                        .map(node -> {
                            String brokerRouteName = ListenersUtils.backwardsCompatiblePerBrokerServiceName(
                                ReconcilerUtils.getControllerNameFromPodName(node.podName()), node.nodeId(), listener);

                            RouteOperator operator = selectRouteOperator(node.clusterId());
                            if (operator == null) {
                                LOGGER.warnCr(reconciliation, "Skipping per-broker route collection for node {} in cluster '{}' - cluster not available", node.nodeId(), node.clusterId());
                                return Future.succeededFuture();
                            }

                            return operator
                                .getAsync(reconciliation.namespace(), brokerRouteName)
                                .compose(route -> {
                                    String host = extractRouteHost(route);
                                    if (host != null) {
                                        LOGGER.debugCr(reconciliation, "Found address {} for Route {}", host, route.getMetadata().getName());

                                        result.brokerDnsNames
                                            .computeIfAbsent(node.nodeId(), k -> new HashSet<>(2))
                                            .add(host);

                                        String advertisedHostname = ListenersUtils.brokerAdvertisedHost(listener, node);
                                        if (advertisedHostname != null) {
                                            result.brokerDnsNames.get(node.nodeId()).add(advertisedHostname);
                                        }

                                        registerAdvertisedHostname(node.nodeId(), listener, advertisedHostname, host);
                                        registerAdvertisedPort(node.nodeId(), listener, KafkaCluster.ROUTE_PORT);
                                    }
                                    return Future.succeededFuture();
                                });
                        }).collect(Collectors.toList());

                    return Future.join(brokerFutures);
                });

            listenerFutures.add(perListenerFuture);
        }

        return Future.join(listenerFutures).mapEmpty();
    }

    /**
     * Safely extracts the host from the Route status.
     */
    private String extractRouteHost(Route route) {
        if (route != null && route.getStatus() != null
                && route.getStatus().getIngress() != null
                && !route.getStatus().getIngress().isEmpty()
                && route.getStatus().getIngress().get(0).getHost() != null) {
            return route.getStatus().getIngress().get(0).getHost();
        }
        return null;
    }

    /**
     * Selects the appropriate RouteOperator based on cluster ID.
     */
    private RouteOperator selectRouteOperator(String clusterId) {
        return centralClusterId.equals(clusterId) ? routeOperator : stretchRouteOperators.get(clusterId);
    }

    /**
     * Makes sure all routes are ready and collects their addresses for Statuses,
     * certificates and advertised addresses. This method for all routes:
     *      1) Checks if the bootstrap route has been provisioned (has a loadbalancer address)
     *      2) Collects the relevant addresses and stores them for use in certificates and in CR status
     *      3) Checks it the broker routes have been provisioned (have an address)
     *      4) Collects the route addresses for certificates and advertised hostnames
     *
     * @return  Future which completes when all Routes are ready and their addresses are collected
     */
    protected Future<Void> routesReady() {
        List<GenericKafkaListener> routeListeners = ListenersUtils.routeListeners(kafka.getListeners());
        List<Future<?>> listenerFutures = new ArrayList<>(routeListeners.size());

        for (GenericKafkaListener listener : routeListeners) {
            String bootstrapRouteName = ListenersUtils.backwardsCompatibleBootstrapRouteOrIngressName(reconciliation.name(), listener);

            @SuppressWarnings({ "rawtypes" }) // Has to use Raw type because of the CompositeFuture
            Future perListenerFut = routeOperator.hasAddress(reconciliation, reconciliation.namespace(), bootstrapRouteName, 1_000, operationTimeoutMs)
                    .compose(res -> routeOperator.getAsync(reconciliation.namespace(), bootstrapRouteName))
                    .compose(route -> {
                        String bootstrapAddress = route.getStatus().getIngress().get(0).getHost();
                        LOGGER.debugCr(reconciliation, "Found address {} for Route {}", bootstrapAddress, bootstrapRouteName);

                        result.bootstrapDnsNames.add(bootstrapAddress);

                        ListenerStatus ls = new ListenerStatusBuilder()
                                .withName(listener.getName())
                                .withAddresses(new ListenerAddressBuilder()
                                        .withHost(bootstrapAddress)
                                        .withPort(KafkaCluster.ROUTE_PORT)
                                        .build())
                                .build();
                        result.listenerStatuses.add(ls);

                        return Future.succeededFuture();
                    })
                    .compose(res -> {
                        List<Future<Void>> perPodFutures = new ArrayList<>();

                        for (NodeRef node : kafka.brokerNodes()) {
                            perPodFutures.add(
                                    routeOperator.hasAddress(reconciliation, reconciliation.namespace(), ListenersUtils.backwardsCompatiblePerBrokerServiceName(ReconcilerUtils.getControllerNameFromPodName(node.podName()), node.nodeId(), listener), 1_000, operationTimeoutMs)
                            );
                        }

                        return Future.join(perPodFutures);
                    })
                    .compose(res -> {
                        List<Future<Void>> perPodFutures = new ArrayList<>();

                        for (NodeRef node : kafka.brokerNodes()) {
                            //final int finalBrokerId = brokerId;
                            Future<Void> perBrokerFut = routeOperator.getAsync(reconciliation.namespace(), ListenersUtils.backwardsCompatiblePerBrokerServiceName(ReconcilerUtils.getControllerNameFromPodName(node.podName()), node.nodeId(), listener))
                                    .compose(route -> {
                                        String brokerAddress = route.getStatus().getIngress().get(0).getHost();
                                        LOGGER.debugCr(reconciliation, "Found address {} for Route {}", brokerAddress, route.getMetadata().getName());

                                        result.brokerDnsNames.computeIfAbsent(node.nodeId(), k -> new HashSet<>(2)).add(brokerAddress);

                                        String advertisedHostname = ListenersUtils.brokerAdvertisedHost(listener, node);
                                        if (advertisedHostname != null) {
                                            result.brokerDnsNames.get(node.nodeId()).add(advertisedHostname);
                                        }

                                        registerAdvertisedHostname(node.nodeId(), listener, advertisedHostname, brokerAddress);
                                        registerAdvertisedPort(node.nodeId(), listener, KafkaCluster.ROUTE_PORT);

                                        return Future.succeededFuture();
                                    });

                            perPodFutures.add(perBrokerFut);
                        }

                        return Future.join(perPodFutures);
                    });

            listenerFutures.add(perListenerFut);
        }

        return Future
                .join(listenerFutures)
                .mapEmpty();
    }

    /**
     * Ensures all Ingresses are ready in all clusters and collects their addresses for Statuses,
     * certificates, and advertised addresses.
     *
     * @return Future completing when all Ingresses are ready and addresses are collected.
     */
    protected Future<Void> stretchIngressesReady() {
        List<GenericKafkaListener> ingressListeners = ListenersUtils.ingressListeners(kafka.getListeners());
        List<Future<Void>> listenerFutures = new ArrayList<>(ingressListeners.size());

        for (GenericKafkaListener listener : ingressListeners) {
            String bootstrapIngressName = ListenersUtils.backwardsCompatibleBootstrapRouteOrIngressName(reconciliation.name(), listener);

            // Step 1: Wait until bootstrap ingress is provisioned in all clusters
            List<Future<Void>> bootstrapIngressReadinessFutures = targetClusterIds.stream()
                .map(clusterId -> selectIngressOperator(clusterId)
                    .hasIngressAddress(reconciliation, reconciliation.namespace(), bootstrapIngressName, 1_000, operationTimeoutMs))
                .collect(Collectors.toList());

            Future<Void> perListenerFuture = Future.join(bootstrapIngressReadinessFutures)
                // Step 2: Collect bootstrap addresses from ingress status
                .compose(ignore -> {
                    for (String clusterId : targetClusterIds) {
                        String bootstrapAddress = listener.getConfiguration().getBootstrap().getHost(); // FIXME: must be replaced with actual ingress status
                        LOGGER.debugCr(reconciliation, "Using address {} for Ingress {}", bootstrapAddress, bootstrapIngressName);
                        result.bootstrapDnsNames.add(bootstrapAddress);

                        ListenerStatus listenerStatus = new ListenerStatusBuilder()
                            .withName(listener.getName())
                            .withAddresses(new ListenerAddressBuilder()
                                .withHost(bootstrapAddress)
                                .withPort(KafkaCluster.INGRESS_PORT)
                                .build())
                            .build();
                        result.listenerStatuses.add(listenerStatus);
                    }

                    // Step 3: Wait for broker ingresses to be ready
                    List<Future<Void>> brokerReadinessFutures = kafka.brokerNodes().stream()
                        .map(node -> selectIngressOperator(node.clusterId())
                            .hasIngressAddress(
                                reconciliation,
                                reconciliation.namespace(),
                                ListenersUtils.backwardsCompatiblePerBrokerServiceName(
                                    ReconcilerUtils.getControllerNameFromPodName(node.podName()), node.nodeId(), listener),
                                1_000,
                                operationTimeoutMs))
                        .collect(Collectors.toList());

                    return Future.join(brokerReadinessFutures);
                })
                // Step 4: Collect broker addresses and register advertised hostnames
                .compose(ignore -> {
                    for (NodeRef node : kafka.brokerNodes()) {
                        String brokerAddress = ListenersUtils.brokerHost(listener, node); // FIXME: same here: don't blindly trust this
                        String brokerIngressName = ListenersUtils.backwardsCompatiblePerBrokerServiceName(
                            ReconcilerUtils.getControllerNameFromPodName(node.podName()), node.nodeId(), listener);

                        LOGGER.debugCr(reconciliation, "Using address {} for Ingress {}", brokerAddress, brokerIngressName);

                        result.brokerDnsNames.computeIfAbsent(node.nodeId(), k -> new HashSet<>(2)).add(brokerAddress);

                        String advertisedHostname = ListenersUtils.brokerAdvertisedHost(listener, node);
                        if (advertisedHostname != null) {
                            result.brokerDnsNames.get(node.nodeId()).add(advertisedHostname);
                        }

                        registerAdvertisedHostname(node.nodeId(), listener, advertisedHostname, brokerAddress);
                        registerAdvertisedPort(node.nodeId(), listener, KafkaCluster.INGRESS_PORT);
                    }
                    return Future.succeededFuture();
                });

            listenerFutures.add(perListenerFuture);
        }

        return Future.join(listenerFutures).mapEmpty();
    }

    private IngressOperator selectIngressOperator(String clusterId) {
        return clusterId.equals(centralClusterId) ? ingressOperator : stretchIngressOperators.get(clusterId);
    }


    /**
     * Makes sure all ingresses are ready and collects their addresses for Statuses,
     * certificates and advertised addresses. This method for all ingresses:
     *      1) Checks if the bootstrap ingress has been provisioned (has a loadbalancer address)
     *      2) Collects the relevant addresses and stores them for use in certificates and in CR status
     *      3) Checks it the broker ingresses have been provisioned (have an address)
     *      4) Collects the route addresses for certificates and advertised hostnames
     *
     * @return  Future which completes when all Ingresses are ready and their addresses are collected
     */
    protected Future<Void> ingressesReady() {

        List<GenericKafkaListener> ingressListeners = ListenersUtils.ingressListeners(kafka.getListeners());
        List<Future<Void>> listenerFutures = new ArrayList<>(ingressListeners.size());

        for (GenericKafkaListener listener : ingressListeners) {
            String bootstrapIngressName = ListenersUtils.backwardsCompatibleBootstrapRouteOrIngressName(reconciliation.name(), listener);

            Future<Void> perListenerFut = ingressOperator.hasIngressAddress(reconciliation, reconciliation.namespace(), bootstrapIngressName, 1_000, operationTimeoutMs)
                    .compose(res -> {
                        String bootstrapAddress = listener.getConfiguration().getBootstrap().getHost();
                        LOGGER.debugCr(reconciliation, "Using address {} for Ingress {}", bootstrapAddress, bootstrapIngressName);

                        result.bootstrapDnsNames.add(bootstrapAddress);

                        ListenerStatus ls = new ListenerStatusBuilder()
                                .withName(listener.getName())
                                .withAddresses(new ListenerAddressBuilder()
                                        .withHost(bootstrapAddress)
                                        .withPort(KafkaCluster.ROUTE_PORT)
                                        .build())
                                .build();
                        result.listenerStatuses.add(ls);

                        // Check if broker ingresses are ready
                        List<Future<Void>> perPodFutures = new ArrayList<>();

                        for (NodeRef node : kafka.brokerNodes()) {
                            perPodFutures.add(
                                    ingressOperator.hasIngressAddress(reconciliation, reconciliation.namespace(), ListenersUtils.backwardsCompatiblePerBrokerServiceName(ReconcilerUtils.getControllerNameFromPodName(node.podName()), node.nodeId(), listener), 1_000, operationTimeoutMs)
                            );
                        }

                        return Future.join(perPodFutures);
                    })
                    .compose(res -> {
                        for (NodeRef node : kafka.brokerNodes()) {
                            String brokerAddress = ListenersUtils.brokerHost(listener, node);
                            LOGGER.debugCr(reconciliation, "Using address {} for Ingress {}", brokerAddress, ListenersUtils.backwardsCompatiblePerBrokerServiceName(ReconcilerUtils.getControllerNameFromPodName(node.podName()), node.nodeId(), listener));

                            result.brokerDnsNames.computeIfAbsent(node.nodeId(), k -> new HashSet<>(2)).add(brokerAddress);

                            String advertisedHostname = ListenersUtils.brokerAdvertisedHost(listener, node);
                            if (advertisedHostname != null) {
                                result.brokerDnsNames.get(node.nodeId()).add(advertisedHostname);
                            }

                            registerAdvertisedHostname(node.nodeId(), listener, advertisedHostname, brokerAddress);
                            registerAdvertisedPort(node.nodeId(), listener, KafkaCluster.INGRESS_PORT);
                        }

                        return Future.succeededFuture();
                    });

            listenerFutures.add(perListenerFut);
        }

        return Future
                .join(listenerFutures)
                .mapEmpty();
    }

    /**
     * Collects the custom listener certificates from the secrets and stores them for later use
     *
     * @return  Future which completes when all custom listener certificates are collected and are valid
     */
    protected Future<Map<String, String>> customListenerCertificates() {
        List<String> secretNames = kafka.getListeners().stream()
                .filter(listener -> listener.isTls()
                        && listener.getConfiguration() != null
                        && listener.getConfiguration().getBrokerCertChainAndKey() != null)
                .map(listener -> listener.getConfiguration().getBrokerCertChainAndKey().getSecretName())
                .distinct()
                .collect(Collectors.toList());
        LOGGER.debugCr(reconciliation, "Validating secret {} with custom TLS listener certificates", secretNames);

        List<Future<Secret>> secretFutures = new ArrayList<>(secretNames.size());
        Map<String, Secret> customSecrets = new HashMap<>(secretNames.size());

        for (String secretName : secretNames)   {
            Future<Secret> fut = secretOperator.getAsync(reconciliation.namespace(), secretName)
                    .compose(secret -> {
                        if (secret != null) {
                            customSecrets.put(secretName, secret);
                            LOGGER.debugCr(reconciliation, "Found secrets {} with custom TLS listener certificate", secretName);
                        }

                        return Future.succeededFuture();
                    });

            secretFutures.add(fut);
        }

        return Future.join(secretFutures)
                .compose(res -> {
                    List<String> errors = new ArrayList<>();
                    Map<String, String> customListenerCertificates = new HashMap<>();

                    for (GenericKafkaListener listener : kafka.getListeners())   {
                        if (listener.isTls()
                                && listener.getConfiguration() != null
                                && listener.getConfiguration().getBrokerCertChainAndKey() != null)  {
                            CertAndKeySecretSource customCert = listener.getConfiguration().getBrokerCertChainAndKey();
                            Secret secret = customSecrets.get(customCert.getSecretName());

                            if (secret != null) {
                                if (!secret.getData().containsKey(customCert.getCertificate())) {
                                    errors.add("Secret " + customCert.getSecretName() + " does not contain certificate under the key " + customCert.getCertificate() + ".");
                                } else if (!secret.getData().containsKey(customCert.getKey())) {
                                    errors.add("Secret " + customCert.getSecretName() + " does not contain custom certificate private key under the key " + customCert.getKey() + ".");
                                } else  {
                                    String publicKey = Util.decodeFromBase64(secret.getData().get(customCert.getCertificate()));
                                    customListenerCertificates.put(listener.getName(), publicKey);
                                    result.customListenerCertificateThumbprints.put(listener.getName(), CertUtils.getCertificateShortThumbprint(secret, customCert.getCertificate()));
                                }
                            } else {
                                errors.add("Secret " + customCert.getSecretName() + " with custom TLS certificate does not exist.");
                            }

                        }
                    }

                    if (errors.isEmpty())   {
                        return Future.succeededFuture(customListenerCertificates);
                    } else {
                        LOGGER.errorCr(reconciliation, "Failed to process Secrets with custom certificates: {}", errors);
                        return Future.failedFuture(new InvalidResourceException("Failed to process Secrets with custom certificates: " + errors));
                    }
                });
    }

    /**
     * Adds certificate to the status of an individual listener. This is a utility method to simplify the code in
     * addCertificatesToListenerStatuses method
     *
     * @param listenerName      Name of the lister to which the certificate belongs
     * @param customCertificate Custom listener certificate or null if custom listener certificate is not used
     * @param caCertificate     The public key from the Cluster CA which is used when no custom listener certificate is set
     */
    private void addCertificateToListenerStatus(String listenerName, String customCertificate, String caCertificate)    {
        ListenerStatus status = result.listenerStatuses
                .stream()
                .filter(listenerStatus -> listenerName.equals(listenerStatus.getName()))
                .findFirst()
                .orElseThrow(() -> new RuntimeException("Status for listener " + listenerName + " not found"));

        status.setCertificates(singletonList(customCertificate != null ? customCertificate : caCertificate));
    }

    /**
     * Adds listener certificates to their statuses. This method goes through all TLS enabled listeners and updates
     * their status with either the custom listener certificate or with the CA certificate. This method should be
     * called only after This method should be called only after customListenerCertificates() which fills in the customListenerCertificates field.
     *
     * @return  Future which completes when all statuses are updated
     */
    protected Future<Void> addCertificatesToListenerStatuses(Map<String, String> customListenerCertificates) {
        String caCertificate = new String(clusterCa.currentCaCertBytes(), StandardCharsets.US_ASCII);

        for (GenericKafkaListener listener : kafka.getListeners())   {
            if (listener.isTls())   {
                LOGGER.debugCr(reconciliation, "Adding certificate to status for listener: {}", listener.getName());
                addCertificateToListenerStatus(listener.getName(), customListenerCertificates.get(listener.getName()), caCertificate);
            }
        }

        return Future.succeededFuture();
    }

    /**
     * Add GC ConfigMap as owner reference to a resource in remote clusters.
     *
     * @param resource Resource to add owner to
     * @param clusterId Cluster ID
     * @return Resource with owner reference added
     */
    private <T extends HasMetadata> T addGCConfigMapOwner(
            final T resource,
            final String clusterId) {

        // Skip central cluster - no owner reference needed
        if (clusterId.equals(centralClusterId)) {
            return resource;
        }

        // Get GC ConfigMap name
        String gcConfigMapName = reconciliation.name() + "-kafka-gc";

        // Get ConfigMapOperator for the cluster
        ConfigMapOperator configMapOp = getConfigMapOperatorForCluster(clusterId);
        if (configMapOp == null) {
            LOGGER.warnCr(reconciliation,
                    "No ConfigMapOperator for cluster {}, cannot set GC owner",
                    clusterId);
            return resource;
        }

        // Fetch GC ConfigMap to get its UID
        ConfigMap gcConfigMap = configMapOp.get(reconciliation.namespace(), gcConfigMapName);
        if (gcConfigMap == null || gcConfigMap.getMetadata().getUid() == null) {
            LOGGER.warnCr(reconciliation,
                    "GC ConfigMap {} not found or has no UID in cluster {}, cannot set owner",
                    gcConfigMapName, clusterId);
            return resource;
        }

        String gcUid = gcConfigMap.getMetadata().getUid();
        
        // Add GC ConfigMap as owner reference
        OwnerReference gcOwner = new OwnerReferenceBuilder()
                .withApiVersion("v1")
                .withKind("ConfigMap")
                .withName(gcConfigMapName)
                .withUid(gcUid)
                .withController(false)
                .withBlockOwnerDeletion(false)
                .build();
        
        // Add owner reference to resource
        List<OwnerReference> owners = new ArrayList<>();
        if (resource.getMetadata().getOwnerReferences() != null) {
            owners.addAll(resource.getMetadata().getOwnerReferences());
        }
        owners.add(gcOwner);
        resource.getMetadata().setOwnerReferences(owners);
        
        LOGGER.debugCr(reconciliation,
                "Added GC ConfigMap {} (UID: {}) as owner for {} {} in cluster {}",
                gcConfigMapName, gcUid, resource.getKind(), resource.getMetadata().getName(), clusterId);
        
        return resource;
    }

    /**
     * Get ConfigMap operator for a specific cluster.
     *
     * @param clusterId Cluster ID
     * @return ConfigMapOperator or null if not found
     */
    private ConfigMapOperator getConfigMapOperatorForCluster(
            final String clusterId) {
        if (clusterId.equals(centralClusterId)) {
            return configMapOperator;
        } else {
            ConfigMapOperator cmOp = stretchConfigMapOperators.get(clusterId);
            return cmOp != null ? cmOp : null;
        }
    }

    /**
     * Class used to carry the result of the reconciliation:
     *   - Prepared listener statuses
     *   - DNS names, ports, advertised addresses and advertised ports
     *   - Custom listener certificates
     */
    public static class ReconciliationResult {
        /**
         * List of ListenerStatus objects for the Kafka custom resource status
         */
        public final List<ListenerStatus> listenerStatuses = new ArrayList<>();

        /**
         * Bootstrap DNS names
         */
        public final Set<String> bootstrapDnsNames = new HashSet<>();

        /**
         * Broker DNS names
         */
        public final Map<Integer, Set<String>> brokerDnsNames = new HashMap<>();

        /**
         * Advertised hostnames
         */
        public final Map<Integer, Map<String, String>> advertisedHostnames = new HashMap<>();

        /**
         * Advertised ports
         */
        public final Map<Integer, Map<String, String>> advertisedPorts = new HashMap<>();

        /**
         * Bootstrap node ports
         */
        public final Map<String, Integer> bootstrapNodePorts = new HashMap<>();

        /**
         * Custom Listener certificates hash stubs to be used for rolling updates when the certificate changes
         */
        public final Map<String, String> customListenerCertificateThumbprints = new HashMap<>();
    }
}
