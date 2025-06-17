package org.apache.camel.component.gearman;

import org.apache.camel.Exchange;
import org.apache.camel.Processor;
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
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Processore Camel che fa polling di job da Gearman
 */
public class GearmanPollingProcessor implements Processor {

    private static final Logger log = LoggerFactory.getLogger(GearmanPollingProcessor.class);
    
    // Cache dei worker per evitare di ricrearli ad ogni call
    private static final Map<String, WorkerInfo> workerCache = new ConcurrentHashMap<>();
    
    @Override
    public void process(Exchange exchange) throws Exception {
        String host = exchange.getIn().getHeader("GearmanHost", String.class);
        Integer port = exchange.getIn().getHeader("GearmanPort", Integer.class);
        String functionName = exchange.getIn().getHeader("GearmanFunction", String.class);
        
        if (host == null || port == null || functionName == null) {
            log.warn("Header mancanti per Gearman polling: host={}, port={}, function={}", host, port, functionName);
            exchange.getIn().setBody(null);
            return;
        }
        
        String workerKey = host + ":" + port + ":" + functionName;
        WorkerInfo workerInfo = workerCache.computeIfAbsent(workerKey, k -> {
            try {
                return createWorker(host, port, functionName);
            } catch (Exception e) {
                log.error("Errore nella creazione del worker Gearman per {}", workerKey, e);
                return null;
            }
        });
        
        if (workerInfo == null) {
            log.warn("Worker Gearman non disponibile per {}", workerKey);
            exchange.getIn().setBody(null);
            return;
        }
        
        // Controlla se ci sono messaggi in coda (non bloccante)
        String job = workerInfo.jobQueue.poll();
        if (job != null && !job.trim().isEmpty()) {
            log.debug("Job ricevuto da Gearman: {}", job);
            exchange.getIn().setBody(job);
            exchange.getIn().setHeader("GearmanFunction", functionName);
            exchange.getIn().setHeader("GearmanHost", host);
            exchange.getIn().setHeader("GearmanPort", port);
        } else {
            // Nessun job disponibile
            exchange.getIn().setBody(null);
        }
    }
    
    private WorkerInfo createWorker(String host, int port, String functionName) throws Exception {
        log.info("Creazione worker Gearman per {}:{}:{}", host, port, functionName);
        
        GearmanWorker worker = new GearmanWorkerImpl();
        BlockingQueue<String> jobQueue = new LinkedBlockingQueue<>();
        
        // Aggiungi il server Gearman
        InetSocketAddress address = new InetSocketAddress(host, port);
        GearmanNIOJobServerConnection connection = new GearmanNIOJobServerConnection(address);
        worker.addServer(connection);
        
        // Registra la funzione - corretto seguendo il pattern del GearmanConsumer
        CamelGearmanFunction.setCurrentJobQueue(jobQueue);
        worker.registerFunction(CamelGearmanFunction.class, 0L);
        
        // Avvia il worker in un thread separato
        Thread workerThread = new Thread(() -> {
            try {
                log.info("Avvio worker Gearman per funzione: {}", functionName);
                worker.work();
            } catch (Exception e) {
                log.error("Errore nel worker Gearman", e);
            }
        });
        workerThread.setName("GearmanWorker-" + functionName);
        workerThread.setDaemon(true);
        workerThread.start();
        
        return new WorkerInfo(worker, jobQueue, workerThread);
    }
    
    // Classe per la funzione Gearman
    public static class CamelGearmanFunction extends AbstractGearmanFunction {
        private static BlockingQueue<String> currentJobQueue;
        private byte[] jobData;
        
        public static void setCurrentJobQueue(BlockingQueue<String> jobQueue) {
            currentJobQueue = jobQueue;
        }
        
        @Override
        public GearmanJobResult executeFunction() {
            try {
                String jobDataStr = new String(jobData != null ? jobData : new byte[0]);
                if (currentJobQueue != null) {
                    currentJobQueue.offer(jobDataStr);
                    log.debug("Job ricevuto e aggiunto alla coda: {}", jobDataStr);
                }
                
                return new GearmanJobResultImpl(null, true, "OK".getBytes(), 
                                              new byte[0], new byte[0], 0, 0);
            } catch (Exception e) {
                log.error("Errore nell'elaborazione del job Gearman", e);
                return new GearmanJobResultImpl(null, false, 
                                              ("Error: " + e.getMessage()).getBytes(), 
                                              new byte[0], new byte[0], 0, 0);
            }
        }
        
        public void setJobData(byte[] data) {
            this.jobData = data;
        }
    }
    
    // Classe helper per mantenere le informazioni del worker
    private static class WorkerInfo {
        final GearmanWorker worker;
        final BlockingQueue<String> jobQueue;
        final Thread workerThread;
        
        WorkerInfo(GearmanWorker worker, BlockingQueue<String> jobQueue, Thread workerThread) {
            this.worker = worker;
            this.jobQueue = jobQueue;
            this.workerThread = workerThread;
        }
    }
} 