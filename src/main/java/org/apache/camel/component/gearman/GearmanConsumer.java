package org.apache.camel.component.gearman;

import org.apache.camel.Exchange;
import org.apache.camel.Processor;
import org.apache.camel.support.ScheduledPollConsumer;
import org.gearman.worker.GearmanWorker;
import org.gearman.worker.GearmanWorkerImpl;
import org.gearman.worker.AbstractGearmanFunction;
import org.gearman.common.GearmanNIOJobServerConnection;
import org.gearman.client.GearmanJobResult;
import org.gearman.client.GearmanJobResultImpl;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.InetSocketAddress;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

/**
 * Gearman consumer that polls for jobs from Gearman server
 */
public class GearmanConsumer extends ScheduledPollConsumer {

    private static final Logger log = LoggerFactory.getLogger(GearmanConsumer.class);
    
    private final GearmanEndpoint endpoint;
    private GearmanWorker worker;
    private BlockingQueue<String> jobQueue;
    private Thread workerThread;
    private static GearmanConsumer currentInstance;

    public GearmanConsumer(GearmanEndpoint endpoint, Processor processor) {
        super(endpoint, processor);
        this.endpoint = endpoint;
        this.jobQueue = new LinkedBlockingQueue<>();
        currentInstance = this;
    }

    @Override
    protected void doStart() throws Exception {
        super.doStart();
        
        worker = new GearmanWorkerImpl();
        
        // Aggiungi il server Gearman usando GearmanNIOJobServerConnection
        InetSocketAddress address = new InetSocketAddress(
                endpoint.getConfiguration().getHost(),
                endpoint.getConfiguration().getPort()
        );
        GearmanNIOJobServerConnection connection = new GearmanNIOJobServerConnection(address);
        worker.addServer(connection);

        // Registra la funzione usando il nome della funzione
        worker.registerFunction(CamelGearmanFunction.class, 0L);
        
        // Avvia il worker in un thread separato
        workerThread = new Thread(() -> {
            try {
                log.info("Starting Gearman worker for function: {}", endpoint.getConfiguration().getFunctionName());
                worker.work();
            } catch (Exception e) {
                log.error("Gearman worker error", e);
            }
        });
        workerThread.setName("GearmanWorker-" + endpoint.getConfiguration().getFunctionName());
        workerThread.setDaemon(true);
        workerThread.start();
    }

    @Override
    protected void doStop() throws Exception {
        if (workerThread != null && workerThread.isAlive()) {
            workerThread.interrupt();
        }
        if (worker != null) {
            worker.shutdown();
        }
        super.doStop();
    }

    @Override
    protected int poll() throws Exception {
        // Controlla se ci sono job in coda
        String job = jobQueue.poll(100, TimeUnit.MILLISECONDS);
        
        if (job != null) {
            Exchange exchange = createExchange(false);
            exchange.getIn().setBody(job);
            exchange.getIn().setHeader("GearmanFunction", endpoint.getConfiguration().getFunctionName());
            exchange.getIn().setHeader("GearmanHost", endpoint.getConfiguration().getHost());
            exchange.getIn().setHeader("GearmanPort", endpoint.getConfiguration().getPort());
            
            try {
                getProcessor().process(exchange);
                return 1; // Indica che è stato processato un messaggio
            } catch (Exception e) {
                exchange.setException(e);
                getExceptionHandler().handleException("Error processing Gearman job", exchange, e);
                return 0;
            }
        }
        
        return 0; // Nessun messaggio processato
    }

    public BlockingQueue<String> getJobQueue() {
        return jobQueue;
    }

    public static GearmanConsumer getCurrentInstance() {
        return currentInstance;
    }

    // Classe concreta per la funzione Gearman
    public static class CamelGearmanFunction extends AbstractGearmanFunction {
        private byte[] jobData;

        @Override
        public GearmanJobResult executeFunction() {
            try {
                // Usa i dati impostati tramite setData
                String jobDataStr = new String(jobData != null ? jobData : new byte[0]);
                
                GearmanConsumer consumer = getCurrentInstance();
                if (consumer != null) {
                    consumer.getJobQueue().offer(jobDataStr);
                    log.debug("Received Gearman job: {}", jobDataStr);
                }
                
                return new GearmanJobResultImpl(null, true, "OK".getBytes(), 
                                              new byte[0], new byte[0], 0, 0);
            } catch (Exception e) {
                log.error("Error processing Gearman job", e);
                return new GearmanJobResultImpl(null, false, 
                                              ("Error: " + e.getMessage()).getBytes(), 
                                              new byte[0], new byte[0], 0, 0);
            }
        }

        public void setJobData(byte[] data) {
            this.jobData = data;
        }
    }
} 