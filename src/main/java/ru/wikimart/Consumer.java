package ru.wikimart;

import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.ILock;
import com.hazelcast.core.Member;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.amqp.core.Message;
import org.springframework.amqp.core.MessageListener;
import org.springframework.amqp.utils.SerializationUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import java.util.Set;


@Component
public class Consumer implements IConsumer, MessageListener {
    @Autowired
    private HazelcastInstance hazelcastInstance;
    Logger log = LoggerFactory.getLogger(Consumer.class);

    public void accept(Task task) {
        final ILock resourceLock = hazelcastInstance.getLock(task.getResourceId());
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
            }
            hazelcastInstance.getCluster().getLocalMember().setBooleanAttribute("BUSY", true);

            if (task.isExclusive()) {
                log.info("Wait until all tasks will be finished");
                waitAllTasksComplete();
                log.info("All tasks are finished");
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
            }
            hazelcastInstance.getCluster().getLocalMember().setBooleanAttribute("BUSY", false);
            log.info("{} is unlocked", task.getResourceId());
        }
    }

    private void waitAllTasksComplete() throws InterruptedException {
        while (isClusterBusy()){
            Thread.sleep(30);
        }
    }

    private boolean isClusterBusy() {
        final Set<Member> members = hazelcastInstance.getCluster().getMembers();
        for (Member m : members){
            if (m != null) {
                final Boolean busy = m.getBooleanAttribute("BUSY");
                if (busy != null && busy && !m.localMember()) {
                    return true;
                }
            }
        }
        return false;
    }

    @Override
    public void onMessage(Message message) {
        log.info("Message received");
        final Task task = (Task)SerializationUtils.deserialize(message.getBody());
        accept(task);
    }
}
