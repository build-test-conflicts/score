package io.cloudslang.worker.management.services;
import io.cloudslang.engine.queue.entities.ExecStatus;
import io.cloudslang.engine.queue.entities.ExecutionMessage;
import io.cloudslang.engine.queue.entities.Payload;
import io.cloudslang.engine.queue.services.QueueDispatcherService;
import io.cloudslang.worker.management.ExecutionsActivityListener;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.log4j.Logger;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.context.ApplicationEvent;
import org.springframework.context.ApplicationListener;
import org.springframework.context.event.ContextClosedEvent;
import org.springframework.context.event.ContextRefreshedEvent;
import javax.annotation.PostConstruct;
import javax.annotation.Resource;
import java.util.List;
import static ch.lambdaj.Lambda.extract;
import static ch.lambdaj.Lambda.on;
import static io.cloudslang.worker.management.services.WorkerConfigurationUtils.NEW_DEFAULT_WORKER_MEMORY_RATIO;
import static java.lang.Double.compare;
import static java.lang.Integer.getInteger;
import static java.lang.Long.parseLong;
import static java.lang.Runtime.getRuntime;
public class InBuffer implements WorkerRecoveryListener, ApplicationListener, Runnable {
  private static Logger logger=Logger.getLogger(InBuffer.class);
  private static int MINIMUM_GC_DELTA=10000;
  @Autowired private QueueDispatcherService queueDispatcher;
  @Resource private String workerUuid;
  @Autowired @Qualifier("inBufferCapacity") private Integer capacity;
  @Autowired(required=false) @Qualifier("coolDownPollingMillis") private Integer coolDownPollingMillis=200;
  @Autowired private WorkerManager workerManager;
  @Autowired private SimpleExecutionRunnableFactory simpleExecutionRunnableFactory;
  @Autowired private OutboundBuffer outBuffer;
  @Autowired private SynchronizationManager syncManager;
  @Autowired @Qualifier("numberOfExecutionThreads") private Integer numberOfThreads;
  @Autowired(required=false) private ExecutionsActivityListener executionsActivityListener;
  @Autowired private WorkerConfigurationUtils workerConfigurationUtils;
  private Thread fillBufferThread=new Thread(this);
  private boolean inShutdown;
  private boolean endOfInit=false;
  private long gcTimer=System.currentTimeMillis();
  private boolean newInBufferBehaviour;
  private int newInBufferSize;
  private int minInBufferSize;
  private double workerFreeMemoryRatio;
  @PostConstruct private void init(){
    capacity=getInteger("worker.inbuffer.capacity",capacity);
    coolDownPollingMillis=getInteger("worker.inbuffer.coolDownPollingMillis",coolDownPollingMillis);
    logger.info("InBuffer capacity is set to :" + capacity + ", coolDownPollingMillis is set to :"+ coolDownPollingMillis);
    newInBufferBehaviour=workerConfigurationUtils.isNewInbuffer();
    logger.info("new inbuffer behaviour enabled: " + newInBufferBehaviour);
    if (newInBufferBehaviour) {
      Pair<Integer,Integer> minSizeAndSizeOfInBuffer=workerConfigurationUtils.getMinSizeAndSizeOfInBuffer(numberOfThreads);
      minInBufferSize=minSizeAndSizeOfInBuffer.getLeft();
      newInBufferSize=minSizeAndSizeOfInBuffer.getRight();
      logger.info("new inbuffer size: " + newInBufferSize);
      logger.info("new inbuffer minimum size: " + minInBufferSize);
    }
    workerFreeMemoryRatio=workerConfigurationUtils.getWorkerMemoryRatio();
    logger.info("Worker free memory ratio is: " + String.format("%.2f",workerFreeMemoryRatio));
  }
  private double getWorkerFreeMemoryRatio(){
    double localWorkerFreeMemoryRatio=workerFreeMemoryRatio;
    return (compare(0,localWorkerFreeMemoryRatio) == 0) ? NEW_DEFAULT_WORKER_MEMORY_RATIO : localWorkerFreeMemoryRatio;
  }
  public void fillBufferPeriodically(){
    while (!inShutdown) {
      try {
        boolean workerUp=workerManager.isUp();
        if (!workerUp) {
          Thread.sleep(3000);
        }
 else {
          syncManager.startGetMessages();
          if (Thread.interrupted()) {
            logger.info("Thread was interrupted while waiting on the lock in fillBufferPeriodically()");
            continue;
          }
          int inBufferSize=workerManager.getInBufferSize();
          if (needToPoll(inBufferSize)) {
            int messagesToGet=!newInBufferBehaviour ? (capacity - inBufferSize) : (newInBufferSize - inBufferSize);
            if (logger.isDebugEnabled()) {
              logger.debug("Polling messages from queue (max " + messagesToGet + ")");
            }
            List<ExecutionMessage> newMessages=queueDispatcher.poll(workerUuid,messagesToGet,workerManager.getFreeMemory());
            if (executionsActivityListener != null) {
              executionsActivityListener.onActivate(extract(newMessages,on(ExecutionMessage.class).getExecStateId()));
            }
            if (logger.isDebugEnabled()) {
              logger.debug("Received " + newMessages.size() + " messages from queue");
            }
            if (!newMessages.isEmpty()) {
              ackMessages(newMessages);
              for (              ExecutionMessage msg : newMessages) {
                addExecutionMessageInner(msg);
              }
              syncManager.finishGetMessages();
              Thread.sleep(coolDownPollingMillis / 8);
            }
 else {
              syncManager.finishGetMessages();
              Thread.sleep(coolDownPollingMillis);
            }
          }
 else {
            syncManager.finishGetMessages();
            Thread.sleep(coolDownPollingMillis);
          }
        }
      }
 catch (      InterruptedException ex) {
        logger.error("Fill InBuffer thread was interrupted... ",ex);
        syncManager.finishGetMessages();
        try {
          Thread.sleep(1000);
        }
 catch (        InterruptedException ignore) {
        }
      }
catch (      Exception ex) {
        logger.error("Failed to load new ExecutionMessages to the buffer!",ex);
        syncManager.finishGetMessages();
        try {
          Thread.sleep(1000);
        }
 catch (        InterruptedException ignore) {
        }
      }
 finally {
        syncManager.finishGetMessages();
      }
    }
  }
  private boolean needToPoll(  int bufferSize){
    if (logger.isDebugEnabled()) {
      logger.debug("InBuffer size: " + bufferSize);
    }
    if (!newInBufferBehaviour) {
      return bufferSize < (capacity * 0.2) && checkFreeMemorySpace();
    }
 else {
      return (bufferSize < minInBufferSize) && checkFreeMemorySpace();
    }
  }
  private void ackMessages(  List<ExecutionMessage> newMessages) throws InterruptedException {
    ExecutionMessage cloned;
    for (    ExecutionMessage message : newMessages) {
      message.setWorkerKey(message.getMsgId() + " : " + message.getExecStateId());
      Payload payload=message.getPayload();
      message.setPayload(null);
      cloned=(ExecutionMessage)message.clone();
      cloned.setStatus(ExecStatus.IN_PROGRESS);
      cloned.incMsgSeqId();
      message.setPayload(payload);
      message.incMsgSeqId();
      outBuffer.put(cloned);
    }
  }
  public void addExecutionMessage(  ExecutionMessage msg) throws InterruptedException {
    try {
      syncManager.startGetMessages();
      if (Thread.currentThread().isInterrupted()) {
        throw new InterruptedException("Thread was interrupted while waiting on the lock in fillBufferPeriodically()!");
      }
      addExecutionMessageInner(msg);
    }
  finally {
      syncManager.finishGetMessages();
    }
  }
  private void addExecutionMessageInner(  ExecutionMessage msg){
    SimpleExecutionRunnable simpleExecutionRunnable=simpleExecutionRunnableFactory.getObject();
    simpleExecutionRunnable.setExecutionMessage(msg);
    long executionId=parseLong(msg.getMsgId());
    workerManager.addExecution(executionId,simpleExecutionRunnable);
  }
  @Override public void onApplicationEvent(  ApplicationEvent applicationEvent){
    if (applicationEvent instanceof ContextRefreshedEvent && !endOfInit) {
      endOfInit=true;
      inShutdown=false;
      fillBufferThread.setName("WorkerFillBufferThread");
      fillBufferThread.start();
    }
 else     if (applicationEvent instanceof ContextClosedEvent) {
      inShutdown=true;
    }
  }
  @Override public void run(){
    fillBufferPeriodically();
  }
  private boolean checkFreeMemorySpace(){
    double allocatedMemory=getRuntime().totalMemory() - getRuntime().freeMemory();
    long maxMemory=getRuntime().maxMemory();
    double presumableFreeMemory=maxMemory - allocatedMemory;
    double crtFreeMemoryRatio=presumableFreeMemory / maxMemory;
    double configuredWorkerFreeMemoryRatio=getWorkerFreeMemoryRatio();
    boolean result=crtFreeMemoryRatio > configuredWorkerFreeMemoryRatio;
    if (!result) {
      logger.warn("InBuffer would not poll messages, because there is not enough free memory. Free memory is " + String.format("%.0f",presumableFreeMemory) + ". Worker free memory ratio is "+ String.format("%.2f",configuredWorkerFreeMemoryRatio));
      if (System.currentTimeMillis() > (gcTimer + MINIMUM_GC_DELTA)) {
        logger.warn("Trying to initiate garbage collection");
        System.gc();
        gcTimer=System.currentTimeMillis();
      }
    }
    return result;
  }
  @Override public void doRecovery(){
    fillBufferThread.interrupt();
  }
  public int getCapacity(){
    return (!newInBufferBehaviour) ? capacity : newInBufferSize;
  }
  public InBuffer(){
  }
}
