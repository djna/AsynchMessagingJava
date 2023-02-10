package org.djna.async;

import javax.jms.*;

public  class Receiver implements Runnable, MessageListener {

    private Session session;
    private String destination;

    public Receiver(Session session, String destination) {
        this.session = session;
        this.destination = destination;
    }

    public void run() {
        try {
            MessageConsumer consumer = session.createConsumer(session.createQueue(destination));
            consumer.setMessageListener(this);
        } catch (JMSException e) {
            throw new RuntimeException(e);
        }
    }

    public void onMessage(Message message) {
        try {
            System.out.println(String.format("received message '%s' with message id '%s'", ((TextMessage) message).getText(), message.getJMSMessageID()));
            message.acknowledge();
        } catch (JMSException e) {
            throw new RuntimeException(e);
        }
    }
}

