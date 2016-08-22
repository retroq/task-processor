package ru.wikimart;

import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.IAtomicLong;
import com.hazelcast.core.ILock;
import com.hazelcast.core.IMap;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.amqp.core.Message;
import org.springframework.amqp.core.MessageListener;
import org.springframework.amqp.utils.SerializationUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;


@Component
public class Consumer implements IConsumer, MessageListener {
    @Autowired
    private HazelcastInstance hazelcastInstance;
    Logger log = LoggerFactory.getLogger(Consumer.class);

    public void accept(Task task) {
        final ILock resourceLock = hazelcastInstance.getLock(task.getResourceId());
        final IAtomicLong tasksCounter = hazelcastInstance.getAtomicLong("tasks-counter");
        final ILock exclusiveTaskLock = hazelcastInstance.getLock("exclusive-task-lock");

        try {
            if (exclusiveTaskLock.isLocked()) {
                //wait until exclusive task will be finished
                log.info("Wait until exclusive task will be finished");
                exclusiveTaskLock.lock();
                exclusiveTaskLock.unlock();
                log.info("Detected finishing of the exclusive task by another thread");
            }
            if (task.isExclusive()){
                log.info("Exclusive task processing");
                exclusiveTaskLock.lock();
            } else {
                tasksCounter.incrementAndGet();
            }
            if (task.isExclusive()) {
                log.info("Wait until all tasks will be finished");
                while (tasksCounter.get() != 0){
                    log.info("Current active tasks = {}", tasksCounter.get());
                    Thread.sleep(3);
                }
                log.info("All tasks is finished");
            }
            if (resourceLock.isLocked()){
                log.info(task.getResourceId() + " is locked");
            }
            resourceLock.lock();
            log.info("Received <" + task + ">");
            Thread.sleep(1100);
        } catch (Exception e) {
            log.error("Error while task processing", e);
        }
        finally {
            if (resourceLock.isLocked()) {
                resourceLock.unlock();
            }
            if (task.isExclusive()){
                exclusiveTaskLock.unlock();
                log.info("All waiting threads are unlocked");
            } else {
                tasksCounter.decrementAndGet();
            }
            log.info("{} is unlocked", task.getResourceId());
        }
    }

    @Override
    public void onMessage(Message message) {
        log.info("Message received");
        final Task task = (Task)SerializationUtils.deserialize(message.getBody());
        accept(task);
    }
}
