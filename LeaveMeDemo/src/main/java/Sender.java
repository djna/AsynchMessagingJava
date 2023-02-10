package org.djna.async;

import javax.jms.JMSException;
import javax.jms.MessageProducer;
import javax.jms.Session;
import javax.jms.TextMessage;
import java.util.UUID;


public  class Sender implements Runnable {

    private Session session;
    private String destination;
    private int howManyToSend;

    public Sender(Session initSession, String initDestination, int initHowManyToSend) {
        this.session = initSession;
        this.destination = initDestination;
        howManyToSend = initHowManyToSend;
    }

    public Sender(Session initSession, String initDestination) {
        this(initSession, initDestination, 10 );
    }

    public void run() {
        try {
            MessageProducer messageProducer = session.createProducer(session.createQueue(destination));
            long counter = 0;

            while (counter < howManyToSend) {
                TextMessage message = session.createTextMessage("Message " + ++counter);
                message.setJMSMessageID(UUID.randomUUID().toString());
                messageProducer.send(message);
                System.out.printf("Sent %d: %s%n", counter, message);
            }
        } catch (JMSException e) {
            throw new RuntimeException(e);
        }
    }
}


