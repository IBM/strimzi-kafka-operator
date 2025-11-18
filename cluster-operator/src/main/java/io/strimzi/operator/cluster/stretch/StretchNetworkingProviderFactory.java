/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.operator.cluster.stretch;

import io.strimzi.operator.cluster.ClusterOperatorConfig;
import io.strimzi.operator.cluster.operator.resource.ResourceOperatorSupplier;
import io.strimzi.operator.cluster.stretch.spi.StretchNetworkingProvider;
import io.strimzi.operator.common.InvalidConfigurationException;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.File;
import java.net.MalformedURLException;
import java.net.URL;
import java.net.URLClassLoader;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * Factory for creating StretchNetworkingProvider instances.
 *
 * <p>All networking providers are loaded as external plugins via className + classPath.
 * This includes Strimzi-maintained providers (NodePort, LoadBalancer) which live in a
 * separate Strimzi repository, as well as third-party providers (e.g., MCS).</p>
 *
 * <h2>Configuration</h2>
 * <p>All providers require explicit configuration using the same environment variables:</p>
 * <pre>{@code
 * # NodePort provider (Strimzi-maintained)
 * STRIMZI_STRETCH_PLUGIN_CLASS_NAME=io.strimzi.operator.cluster.stretch.NodePortNetworkingProvider
 * STRIMZI_STRETCH_PLUGIN_CLASS_PATH=/opt/strimzi/plugins/stretch-networking/*
 *
 * # LoadBalancer provider (Strimzi-maintained)
 * STRIMZI_STRETCH_PLUGIN_CLASS_NAME=io.strimzi.operator.cluster.stretch.LoadBalancerNetworkingProvider
 * STRIMZI_STRETCH_PLUGIN_CLASS_PATH=/opt/strimzi/plugins/stretch-networking/*
 *
 * # MCS provider (third-party)
 * STRIMZI_STRETCH_PLUGIN_CLASS_NAME=com.example.MCSNetworkingProvider
 * STRIMZI_STRETCH_PLUGIN_CLASS_PATH=/opt/strimzi/plugins/mcs/*
 * }</pre>
 *
 * <p><b>Note:</b> All providers use the same loading mechanism and environment variables.
 * There is no default provider - configuration is required.</p>
 *
 * @since 0.47.0
 */
public final class StretchNetworkingProviderFactory {

    /**
     * Logger for this class.
     */
    private static final Logger LOGGER =
            LogManager.getLogger(StretchNetworkingProviderFactory.class);

    /**
     * Private constructor to prevent instantiation.
     */
    private StretchNetworkingProviderFactory() {
        // Utility class
    }

    /**
     * Create a networking provider based on configuration.
     *
     * @param operatorConfig Cluster operator configuration
     * @param config Provider-specific configuration
     * @param centralSupplier ResourceOperatorSupplier for central cluster
     * @param remoteResourceOperatorSupplier Supplier for remote cluster operators
     * @return Initialized provider
     * @throws InvalidConfigurationException if configuration is invalid
     */
    public static StretchNetworkingProvider create(
            final ClusterOperatorConfig operatorConfig,
            final Map<String, String> config,
            final ResourceOperatorSupplier centralSupplier,
            final RemoteResourceOperatorSupplier remoteResourceOperatorSupplier) {

        String className = operatorConfig.getStretchPluginClassName();
        String classPath = operatorConfig.getStretchPluginClassPath();

        // All providers require explicit configuration
        if (className == null || className.isEmpty()) {
            throw new InvalidConfigurationException(
                "Stretch networking requires a plugin to be configured. " +
                "Please set STRIMZI_STRETCH_PLUGIN_CLASS_NAME and STRIMZI_STRETCH_PLUGIN_CLASS_PATH. " +
                "Examples:\n" +
                "  NodePort: STRIMZI_STRETCH_PLUGIN_CLASS_NAME=io.strimzi.operator.cluster.stretch.NodePortNetworkingProvider\n" +
                "  LoadBalancer: STRIMZI_STRETCH_PLUGIN_CLASS_NAME=io.strimzi.operator.cluster.stretch.LoadBalancerNetworkingProvider\n" +
                "  MCS: STRIMZI_STRETCH_PLUGIN_CLASS_NAME=com.example.MCSNetworkingProvider");
        }

        LOGGER.info("Loading stretch networking provider: {} from {}",
                   className, classPath != null ? classPath : "default classpath");
        
        StretchNetworkingProvider provider = loadCustomProvider(className, classPath);
        
        // Initialize the provider
        try {
            provider.init(config, centralSupplier, remoteResourceOperatorSupplier)
                    .toCompletionStage().toCompletableFuture().get();
            LOGGER.info("Stretch networking provider '{}' initialized successfully",
                    provider.getProviderName());
        } catch (Exception e) {
            throw new InvalidConfigurationException(
                    "Failed to initialize stretch networking provider '" + className + "': " + e.getMessage(), e);
        }

        return provider;
    }


    /**
     * Load a custom provider by class name with optional custom classpath.
     *
     * @param className Fully qualified class name
     * @param classPath Optional classpath (supports wildcards like /opt/plugins/*)
     * @return Provider instance
     * @throws InvalidConfigurationException if class cannot be loaded or instantiated
     */
    private static StretchNetworkingProvider loadCustomProvider(
            final String className,
            final String classPath) {
        try {
            // Create custom ClassLoader if classPath is provided
            ClassLoader classLoader;
            if (classPath != null && !classPath.isEmpty()) {
                URL[] urls = parseClassPath(classPath);
                LOGGER.debug("Creating URLClassLoader with {} URLs", urls.length);
                classLoader = new URLClassLoader(urls, StretchNetworkingProviderFactory.class.getClassLoader());
            } else {
                // Use default classpath
                classLoader = StretchNetworkingProviderFactory.class.getClassLoader();
            }

            // Load the class
            Class<?> providerClass = classLoader.loadClass(className);
            LOGGER.debug("Loaded class: {}", providerClass.getName());

            // Verify it implements the interface
            if (!StretchNetworkingProvider.class.isAssignableFrom(providerClass)) {
                throw new InvalidConfigurationException(
                    "Class '" + className + "' does not implement StretchNetworkingProvider interface");
            }

            // Instantiate (requires no-arg constructor)
            StretchNetworkingProvider provider =
                (StretchNetworkingProvider) providerClass.getDeclaredConstructor().newInstance();

            LOGGER.info("Successfully loaded custom provider: {} ({})",
                       provider.getProviderName(), className);

            return provider;

        } catch (ClassNotFoundException e) {
            throw new InvalidConfigurationException(
                    "Custom networking provider class not found: '" + className + "'. " +
                    "Make sure the plugin JAR is in the classpath or specify STRIMZI_STRETCH_PLUGIN_CLASS_PATH", e);
        } catch (NoSuchMethodException e) {
            throw new InvalidConfigurationException(
                    "Custom networking provider '" + className + "' must have a public no-argument constructor", e);
        } catch (Exception e) {
            throw new InvalidConfigurationException(
                    "Failed to load custom networking provider '" + className + "': " + e.getMessage(), e);
        }
    }

    /**
     * Parse classPath string to URL array.
     *
     * <p>Supports:</p>
     * <ul>
     *   <li>Single JAR: {@code /opt/strimzi/plugins/my-plugin.jar}</li>
     *   <li>Directory with wildcard: {@code /opt/strimzi/plugins/*}</li>
     *   <li>Multiple paths: {@code /path1/plugin.jar:/path2/*}</li>
     * </ul>
     *
     * @param classPath ClassPath string
     * @return Array of URLs
     * @throws InvalidConfigurationException if classPath is invalid
     */
    private static URL[] parseClassPath(String classPath) {
        List<URL> urls = new ArrayList<>();

        try {
            for (String path : classPath.split(":")) {
                path = path.trim();

                if (path.isEmpty()) {
                    continue;
                }

                if (path.endsWith("/*")) {
                    // Handle wildcard - add all JARs in directory
                    String dirPath = path.substring(0, path.length() - 2);
                    File dir = new File(dirPath);

                    if (!dir.exists()) {
                        LOGGER.warn("Plugin directory does not exist: {}", dirPath);
                        continue;
                    }

                    if (!dir.isDirectory()) {
                        LOGGER.warn("Plugin path is not a directory: {}", dirPath);
                        continue;
                    }

                    File[] jars = dir.listFiles((d, name) -> name.endsWith(".jar"));
                    if (jars != null && jars.length > 0) {
                        for (File jar : jars) {
                            urls.add(jar.toURI().toURL());
                            LOGGER.debug("Added plugin JAR to classpath: {}", jar.getAbsolutePath());
                        }
                    } else {
                        LOGGER.warn("No JAR files found in plugin directory: {}", dirPath);
                    }
                } else {
                    // Single file or directory
                    File file = new File(path);
                    if (file.exists()) {
                        urls.add(file.toURI().toURL());
                        LOGGER.debug("Added to classpath: {}", file.getAbsolutePath());
                    } else {
                        LOGGER.warn("Plugin path does not exist: {}", path);
                    }
                }
            }

            if (urls.isEmpty()) {
                throw new InvalidConfigurationException(
                    "No valid plugin JARs found in classPath: " + classPath);
            }

        } catch (MalformedURLException e) {
            throw new InvalidConfigurationException(
                    "Failed to parse plugin classPath: " + classPath, e);
        }

        return urls.toArray(new URL[0]);
    }

}

