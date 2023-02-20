import com.google.gson.Gson;

import javax.jms.*;
import java.util.HashMap;
import java.util.Map;
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
            messageProducer.setPriority(3);
            messageProducer.setDeliveryMode(DeliveryMode.NON_PERSISTENT);
            //messageProducer.setTimeToLive(60000);
            long counter = 0;

            while (counter < howManyToSend) {
                Map info = new HashMap();
                info.put("user", "DaveA");
                info.put("search", "hotel");
                info.put("city", "Valencia, ESP");
                String [] criterionArray = {"budget", "breakfast"};
                info.put("criteria", criterionArray );
                Gson gson = new Gson();
                String payload = gson.toJson(info);

                TextMessage message = session.createTextMessage(payload);
                message.setJMSMessageID(UUID.randomUUID().toString());
                message.setJMSCorrelationID("message:" + counter++);

                Destination replyTo = session.createQueue("wibble");
                message.setJMSReplyTo(replyTo);

                messageProducer.send(message);
                System.out.printf("Sent %d: %s%n", counter, message);
            }
        } catch (JMSException e) {
            throw new RuntimeException(e);
        }
    }
}


