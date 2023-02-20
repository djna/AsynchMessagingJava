import com.google.gson.Gson;
import com.google.gson.reflect.TypeToken;

import javax.jms.*;
import java.util.HashMap;
import java.util.Locale;
import java.util.Map;

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
            String payload = ((TextMessage) message).getText();
            Gson gson = new Gson();
            Map map = gson.fromJson(payload, HashMap.class);
            String user = map.get("user").toString();
            System.out.println(String.format("User %s", user ) );
            // example of a bad message, if the user is called "poison"
            boolean isPoison = "poison".equalsIgnoreCase(user);
            if ( isPoison){
                System.out.println(String.format("Bad message, user: %s", user ) );
                session.rollback();
                throw new RuntimeException("Poison!");
            }
            message.acknowledge();
        } catch (JMSException e) {

            throw new RuntimeException(e);
        }
    }
}

