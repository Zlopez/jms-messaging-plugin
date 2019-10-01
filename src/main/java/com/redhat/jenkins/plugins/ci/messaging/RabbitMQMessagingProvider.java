package com.redhat.jenkins.plugins.ci.messaging;

import hudson.Extension;
import hudson.ExtensionList;
import hudson.model.Descriptor;
import jenkins.model.Jenkins;
import org.kohsuke.stapler.DataBoundConstructor;

import java.util.logging.Logger;

import com.rabbitmq.client.ConnectionFactory;
import com.rabbitmq.client.Connection;

import com.redhat.jenkins.plugins.ci.authentication.rabbitmq.RabbitMQAuthenticationMethod.AuthenticationMethodDescriptor;
import com.redhat.jenkins.plugins.ci.authentication.rabbitmq.RabbitMQAuthenticationMethod;
import com.redhat.jenkins.plugins.ci.provider.data.ProviderData;
import com.redhat.jenkins.plugins.ci.provider.data.RabbitMQProviderData;

public class RabbitMQMessagingProvider extends JMSMessagingProvider {

    private static final long serialVersionUID = 82154526798596907L;

    private static final Logger log = Logger.getLogger(RabbitMQMessagingProvider.class.getName());

    private String virtualHost;
    private String hostName;
    private Integer portNumber;
    private RabbitMQAuthenticationMethod authenticationMethod;
    private Connection connection;

    @DataBoundConstructor
    public RabbitMQMessagingProvider(String name, String virtualHost,
                                     String hostName, Integer portNumber,
                                     String topic, RabbitMQAuthenticationMethod authenticationMethod) {
        this.virtualHost = virtualHost;
        this.hostName = hostName;
        this.portNumber = portNumber;
        this.topic = topic;
        this.name = name;
        this.authenticationMethod = authenticationMethod;
    }

    public String getVirtualHost() { return virtualHost; }

    public String getHostName() { return hostName; }

    public Integer getPortNumber() { return portNumber; }

    public String getTopic() { return topic; }

    public Connection getConnection() { return connection; }

    public void setConnection(Connection connection) { this.connection = connection; }

    public RabbitMQAuthenticationMethod getAuthenticationMethod() { return authenticationMethod; }

    public ConnectionFactory getConnectionFactory() {
        return authenticationMethod.getConnectionFactory(getHostName(), getPortNumber(), getVirtualHost());
    }

    @Override
    public JMSMessagingWorker createWorker(ProviderData pdata, String jobname) {
        return new RabbitMQMessagingWorker(this, ((RabbitMQProviderData)pdata).getOverrides(), jobname);
    }

    @Override
    public JMSMessageWatcher createWatcher(String jobname) {
        return new RabbitMQMessageWatcher(jobname);
    }

    @Override
    public Descriptor<JMSMessagingProvider> getDescriptor() {
        return Jenkins.getInstance().getDescriptorByType(RabbitMQMessagingProvider.RabbitMQProviderDescriptor.class);
    }

    @Extension
    public static class RabbitMQProviderDescriptor extends MessagingProviderDescriptor {
        @Override
        public String getDisplayName() {
            return "RabbitMQ";
        }

        public ExtensionList<AuthenticationMethodDescriptor> getAuthenticationMethodDescriptors() {
            return AuthenticationMethodDescriptor.all();
        }
    }
}