package org.apache.camel.component.gearman;

import org.apache.camel.Endpoint;
import org.apache.camel.spi.Metadata;
import org.apache.camel.spi.annotations.Component;
import org.apache.camel.support.DefaultComponent;

import java.util.Map;

/**
 * Represents the component that manages {@link GearmanEndpoint}.
 */
@Component("gearman")
public class GearmanComponent extends DefaultComponent {

    @Metadata(label = "security", secret = true)
    private String host = "localhost";
    
    @Metadata(label = "security")
    private int port = 4730;

    public GearmanComponent() {
    }

    @Override
    protected Endpoint createEndpoint(String uri, String remaining, Map<String, Object> parameters) throws Exception {
        GearmanConfiguration configuration = new GearmanConfiguration();
        configuration.setHost(getHost());
        configuration.setPort(getPort());
        
        GearmanEndpoint endpoint = new GearmanEndpoint(uri, this, configuration);
        setProperties(endpoint, parameters);
        
        return endpoint;
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
} 