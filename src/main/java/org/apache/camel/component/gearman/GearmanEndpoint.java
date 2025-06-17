package org.apache.camel.component.gearman;

import org.apache.camel.Category;
import org.apache.camel.Consumer;
import org.apache.camel.Processor;
import org.apache.camel.Producer;
import org.apache.camel.spi.UriEndpoint;
import org.apache.camel.spi.UriParam;
import org.apache.camel.support.DefaultEndpoint;

/**
 * Gearman endpoint for receiving jobs from Gearman server
 */
@UriEndpoint(firstVersion = "4.0.0", scheme = "gearman", title = "Gearman", 
             syntax = "gearman:functionName", category = {Category.MESSAGING})
public class GearmanEndpoint extends DefaultEndpoint {

    @UriParam
    private GearmanConfiguration configuration;

    public GearmanEndpoint(String uri, GearmanComponent component, GearmanConfiguration configuration) {
        super(uri, component);
        this.configuration = configuration;
    }

    @Override
    public Producer createProducer() throws Exception {
        throw new UnsupportedOperationException("Gearman producer not implemented yet");
    }

    @Override
    public Consumer createConsumer(Processor processor) throws Exception {
        GearmanConsumer consumer = new GearmanConsumer(this, processor);
        configureConsumer(consumer);
        return consumer;
    }

    public GearmanConfiguration getConfiguration() {
        return configuration;
    }

    public void setConfiguration(GearmanConfiguration configuration) {
        this.configuration = configuration;
    }
} 