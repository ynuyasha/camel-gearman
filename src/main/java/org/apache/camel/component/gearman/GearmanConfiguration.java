package org.apache.camel.component.gearman;

import org.apache.camel.spi.Metadata;
import org.apache.camel.spi.UriParam;
import org.apache.camel.spi.UriParams;

/**
 * Configuration for Gearman component
 */
@UriParams
public class GearmanConfiguration {

    @UriParam
    @Metadata(required = true)
    private String functionName;

    @UriParam(defaultValue = "localhost")
    private String host = "localhost";

    @UriParam(defaultValue = "4730")
    private int port = 4730;

    @UriParam(defaultValue = "1000")
    private long pollInterval = 1000;

    public String getFunctionName() {
        return functionName;
    }

    /**
     * Name of the Gearman function to register for
     */
    public void setFunctionName(String functionName) {
        this.functionName = functionName;
    }

    public String getHost() {
        return host;
    }

    /**
     * Hostname of the Gearman server
     */
    public void setHost(String host) {
        this.host = host;
    }

    public int getPort() {
        return port;
    }

    /**
     * Port of the Gearman server
     */
    public void setPort(int port) {
        this.port = port;
    }

    public long getPollInterval() {
        return pollInterval;
    }

    /**
     * Interval in milliseconds to check for new jobs
     */
    public void setPollInterval(long pollInterval) {
        this.pollInterval = pollInterval;
    }
} 