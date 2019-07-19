package com.redhat.jenkins.plugins.ci.messaging;

import com.rabbitmq.client.*;
import com.redhat.jenkins.plugins.ci.CIEnvironmentContributingAction;
import com.redhat.jenkins.plugins.ci.messaging.checks.MsgCheck;
import com.redhat.jenkins.plugins.ci.messaging.data.RabbitMQMessage;
import com.redhat.jenkins.plugins.ci.messaging.data.SendResult;
import com.redhat.jenkins.plugins.ci.provider.data.ProviderData;
import com.redhat.jenkins.plugins.ci.provider.data.RabbitMQPublisherProviderData;
import com.redhat.jenkins.plugins.ci.provider.data.RabbitMQSubscriberProviderData;
import com.redhat.utils.PluginUtils;
import hudson.EnvVars;
import hudson.model.Result;
import hudson.model.Run;
import hudson.model.TaskListener;

import jenkins.model.Jenkins;
import org.apache.commons.lang.exception.ExceptionUtils;
import org.apache.commons.lang3.StringUtils;
import org.codehaus.jackson.map.ObjectMapper;

import java.io.IOException;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.TimeUnit;
import java.util.logging.Level;
import java.util.logging.Logger;

public class RabbitMQMessagingWorker extends JMSMessagingWorker {

    private static final Logger log = Logger.getLogger(RabbitMQMessagingWorker.class.getName());
    private final RabbitMQMessagingProvider provider;

    private Connection connection;
    private Channel channel;
    // Thread interruption flag
    private boolean interrupt = false;
    private String uuid = UUID.randomUUID().toString();
    // Concurrent message queue used for saving messages from consumer
    private ConcurrentLinkedQueue<RabbitMQMessage> messageQueue = new ConcurrentLinkedQueue<RabbitMQMessage>();
    private String consumerTag = "";

    private static final String EXCHANGE_NAME = "amq.topic";

    public RabbitMQMessagingWorker(JMSMessagingProvider messagingProvider, MessagingProviderOverrides overrides, String jobname) {
        super(messagingProvider, overrides, jobname);
        this.provider = (RabbitMQMessagingProvider) messagingProvider;
        this.connection = provider.getConnection();
        this.topic = getTopic(provider);
    }

    @Override
    public boolean subscribe(String jobname, String selector) {
        if (interrupt) {
            return true;
        }
        if (this.topic != null) {
            while (!Thread.currentThread().isInterrupted()) {
                try {
                    if (connection == null || !connection.isOpen()) {
                        if (!connect()) {
                            return false;
                        }
                    }
                    if (channel == null) {
                        this.channel = connection.createChannel();
                        log.info("Subscribing job '" + jobname + "' to " + this.topic + " topic.");
                        channel.exchangeDeclare(EXCHANGE_NAME, "topic", true);
                        String queueName = channel.queueDeclare().getQueue();
                        channel.queueBind(queueName, EXCHANGE_NAME, this.topic);

                        // Create deliver callback to listen for messages
                        DeliverCallback deliverCallback = (consumerTag, delivery) -> {
                            String json = new String(delivery.getBody(), "UTF-8");
                            RabbitMQMessage message = new RabbitMQMessage(delivery.getEnvelope().getRoutingKey(), json);
                            message.setTimestamp(new Date().getTime());
                            messageQueue.add(message);

                        };
                        this.consumerTag = channel.basicConsume(queueName, deliverCallback, (CancelCallback) null);
                        log.info("Successfully subscribed job '" + jobname + "' to topic '" + this.topic + "'.");
                    } else {
                        log.info("Already subscribed job '" + jobname + "' to topic '" + this.topic + "'.");
                    }
                    return true;
                } catch (Exception ex) {

                    // Either we were interrupted, or something else went
                    // wrong. If we were interrupted, then we will jump ship
                    // on the next iteration. If something else happened,
                    // then we just unsubscribe here, sleep, so that we may
                    // try again on the next iteration.

                    log.log(Level.SEVERE, "Eexception raised while subscribing job '" + jobname + "', retrying in " + RETRY_MINUTES + " minutes.", ex);
                    if (!Thread.currentThread().isInterrupted()) {

                        unsubscribe(jobname);

                        try {
                            Thread.sleep(RETRY_MINUTES * 60 * 1000);
                        } catch (InterruptedException ie) {
                            // We were interrupted while waiting to retry.
                            // We will jump ship on the next iteration.

                            // NB: The interrupt flag was cleared when
                            // InterruptedException was thrown. We have to
                            // re-install it to make sure we eventually
                            // leave this thread.
                            Thread.currentThread().interrupt();
                        }
                    }
                }
            }
        }
        return false;
    }

    @Override
    public void unsubscribe(String jobname) {
        if (interrupt) {
            log.info("We are being interrupted. Skipping unsubscribe...");
            return;
        }
        try {
            channel.basicCancel(consumerTag);
            channel.close();
        } catch (Exception ex) {
            log.warning("Exception occurred when closing channel: " + ex.getMessage());
        }

    }

    @Override
    public void receive(String jobname, ProviderData pdata) {
        RabbitMQSubscriberProviderData pd = (RabbitMQSubscriberProviderData) pdata;
        Integer timeout = (pd.getTimeout() != null ? pd.getTimeout() : RabbitMQSubscriberProviderData.DEFAULT_TIMEOUT_IN_MINUTES) * 60 * 1000;

        if (interrupt) {
            log.info("we have been interrupted at start of receive");
            return;
        }

        // subscribe job
        while (!subscribe(jobname)) {
            if (!Thread.currentThread().isInterrupted()) {
                try {
                    int WAIT_SECONDS = 2;
                    Thread.sleep(WAIT_SECONDS * 1000);
                } catch (InterruptedException e) {
                    // We were interrupted while waiting to retry. We will
                    // jump ship on the next iteration.

                    // NB: The interrupt flag was cleared when
                    // InterruptedException was thrown. We have to
                    // re-install it to make sure we eventually leave this
                    // thread.
                    Thread.currentThread().interrupt();
                }
            }
        }

        long lastSeenMessage = new Date().getTime();

        try {
            while ((new Date().getTime() - lastSeenMessage) < timeout) {
                if (!messageQueue.isEmpty()) {
                    RabbitMQMessage data = messageQueue.poll();
                    // Reset timer
                    lastSeenMessage = data.getTimestamp().getTime();
                    //
                    if (provider.verify(data.getBodyJson(), pd.getChecks(), jobname)) {
                        Map<String, String> params = new HashMap<String, String>();
                        params.put("CI_MESSAGE", data.getBodyJson());
                        trigger(jobname, data.getBodyJson(), params);
                    }
                } else {
                    if (interrupt) {
                        log.info("We have been interrupted...");
                        break;
                    }
                }
            }
        } catch (Exception e) {
            if (!Thread.currentThread().isInterrupted()) {
                // Something other than an interrupt causes this.
                // Unsubscribe, but stay in our loop and try to reconnect..
                log.log(Level.WARNING, "JMS exception raised, going to re-subscribe for job '" + jobname + "'.", e);
                unsubscribe(jobname); // Try again next time.
            }
        }
    }

    @Override
    public boolean connect() {
        ConnectionFactory connectionFactory = provider.getConnectionFactory();
        Connection connectiontmp = null;
        try {
            connectiontmp = connectionFactory.newConnection();
            String url = "";
            if (Jenkins.getInstanceOrNull() != null) {
                url = Jenkins.get().getRootUrl();
            }
            connectiontmp.setId(provider.getName() + "_" + url + "_" + uuid + "_" + jobname);
        } catch (Exception e) {
            log.severe("Unable to connect to " + provider.getHostName() + ":" + provider.getPortNumber() + " " + e.getMessage());
            return false;
        }
        log.info("Connection created");
        connection = connectiontmp;
        provider.setConnection(connection);
        return true;
    }

    @Override
    public void disconnect() {
        try {
            channel.close();
        } catch (Exception ex) {
            log.warning("Exception occurred when closing channel: " + ex.getMessage());
        }
        try {
            connection.close();
        } catch (Exception ex) {
            log.warning("Exception occurred when closing connection: " + ex.getMessage());
        }
    }

    @Override
    public SendResult sendMessage(Run<?, ?> build, TaskListener listener, ProviderData pdata) {
        RabbitMQPublisherProviderData pd = (RabbitMQPublisherProviderData)pdata;
        try {
            if (connection == null || !connection.isOpen()) {
                connect();
            }
            if (channel == null) {
                this.channel = connection.createChannel();
                log.info("Channel created.");
            }
        } catch (Exception ex) {
            ex.printStackTrace();
        }
        String body = "";
        String msgId = "";
        try {

            EnvVars env = new EnvVars();
            env.putAll(build.getEnvironment(listener));
            env.put("CI_NAME", build.getParent().getName());
            if (!build.isBuilding()) {
                env.put("CI_STATUS", (build.getResult() == Result.SUCCESS ? "passed" : "failed"));
                env.put("BUILD_STATUS", build.getResult().toString());
            }

            RabbitMQMessage msg = new RabbitMQMessage(PluginUtils.getSubstitutedValue(getTopic(provider), build.getEnvironment(listener)),
                                                     PluginUtils.getSubstitutedValue(pd.getMessageContent(), env));

            msg.setTimestamp(System.currentTimeMillis());

            body = msg.toJson(); // Use toString() instead of getBodyJson so that message ID is included and sent.
            msgId = msg.getMsgId();
            try {
                channel.exchangeDeclare(EXCHANGE_NAME, "topic", true);
                channel.basicPublish(EXCHANGE_NAME, msg.getTopic(), MessageProperties.PERSISTENT_TEXT_PLAIN, body.getBytes());
            } catch (IOException e) {
                if (pd.isFailOnError()) {
                    log.severe("Unhandled exception in perform: Failed to send message!");
                    return new SendResult(false, msgId, body);
                }
            }
            log.fine("JSON message body:\n" + body);
            listener.getLogger().println("JSON message body:\n" + body);

        } catch (Exception e) {
            if (pd.isFailOnError()) {
                log.severe("Unhandled exception in perform: ");
                log.severe(ExceptionUtils.getStackTrace(e));
                listener.fatalError("Unhandled exception in perform: ");
                listener.fatalError(ExceptionUtils.getStackTrace(e));
                return new SendResult(false, msgId, body);
            } else {
                log.warning("Unhandled exception in perform: ");
                log.warning(ExceptionUtils.getStackTrace(e));
                listener.error("Unhandled exception in perform: ");
                listener.error(ExceptionUtils.getStackTrace(e));
                return new SendResult(true, msgId, body);
            }
        } finally {
            try {
                channel.close();
            } catch (Exception e) {
                log.warning("Unhandled exception when closing channel: ");
                log.warning(ExceptionUtils.getStackTrace(e));
                listener.getLogger().println("exception in finally");
            }
        }
        return new SendResult(true, msgId, body);
    }

    @Override
    public String waitForMessage(Run<?, ?> build, TaskListener listener, ProviderData pdata) {
        RabbitMQSubscriberProviderData pd = (RabbitMQSubscriberProviderData)pdata;
        String queue;

        try {
            if (connection == null || !connection.isOpen()) {
                connect();
            }
            if (channel == null) {
                this.channel = connection.createChannel();
            }
            channel.exchangeDeclare(EXCHANGE_NAME, "topic", true);
            queue = channel.queueDeclare().getQueue();
            channel.queueBind(queue, EXCHANGE_NAME, this.topic);
        } catch (Exception ex) {
            log.severe("Connection to broker can't be established!");
            log.severe(ExceptionUtils.getStackTrace(ex));
            listener.error("Connection to broker can't be established!");
            listener.error(ExceptionUtils.getStackTrace(ex));
            return null;
        }

        log.info("Waiting for message.");
        listener.getLogger().println("Waiting for message.");
        for (MsgCheck msgCheck: pd.getChecks()) {
            log.info(" with check: " + msgCheck.toString());
            listener.getLogger().println(" with check: " + msgCheck.toString());
        }
        Integer timeout = (pd.getTimeout() != null ? pd.getTimeout() : RabbitMQSubscriberProviderData.DEFAULT_TIMEOUT_IN_MINUTES);
        log.info(" with timeout: " + timeout + " minutes");
        listener.getLogger().println(" with timeout: " + timeout + " minutes");


        // Create deliver callback to listen for messages
        DeliverCallback deliverCallback = (consumerTag, delivery) -> {
            String json = new String(delivery.getBody(), "UTF-8");
            listener.getLogger().println(
                    "Received '" + delivery.getEnvelope().getRoutingKey() + "':'" + json + "'");
            log.info("Received '" + delivery.getEnvelope().getRoutingKey() + "':'" + json + "'");
            RabbitMQMessage message = new RabbitMQMessage(delivery.getEnvelope().getRoutingKey(), json);
            message.setTimestamp(new Date().getTime());
            messageQueue.add(message);

        };

        String consumerTag = null;

        long startTime = new Date().getTime();

        int timeoutInMs = timeout * 60 * 1000;

        try {
            consumerTag = channel.basicConsume(queue, deliverCallback, (CancelCallback) null);
            while ((new Date().getTime() - startTime) < timeoutInMs) {
                if (!messageQueue.isEmpty()) {

                    String message = messageQueue.poll().getBodyJson();

                    if (!provider.verify(message, pd.getChecks(), jobname)) {
                        continue;
                    }

                    if (build != null) {
                        if (StringUtils.isNotEmpty(pd.getVariable())) {
                            EnvVars vars = new EnvVars();
                            vars.put(pd.getVariable(), message);
                            build.addAction(new CIEnvironmentContributingAction(vars));
                        }
                    }
                    return message;
                }
                if (interrupt) {
                    return null;
                }
                TimeUnit.SECONDS.sleep(1);
            }
            log.severe("Timed out waiting for message!");
            listener.getLogger().println("Timed out waiting for message!");
        } catch (Exception e) {
            log.log(Level.SEVERE, "Unhandled exception waiting for message.", e);
        } finally {
            try {
                if (consumerTag != null) {
                    channel.basicCancel(consumerTag);
                }
                channel.close();
            } catch (Exception e) {
                listener.getLogger().println("exception in finally");
            }
        }
        return null;
    }

    public void prepareForInterrupt() {
        interrupt = true;
    }

    @Override
    public String getDefaultTopic() {
        return null;
    }

}
