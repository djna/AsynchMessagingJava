import com.google.gson.Gson;
import org.apache.activemq.ActiveMQConnectionFactory;

import javax.jms.*;
import java.time.Instant;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;

public class Requester {
    // the queue we will use for responses
    private static String responseQueue = "bookingResponse";
    // a record of all the requests we've sent
    // mapping from correlation Id to the payload record
    private static Map<String, Map<String, String>> sentRequests =  new HashMap<>();

    public static void main(String[] args) throws Exception {
        String clientId = "Requester";
        if (args.length > 0) {
            clientId = args[0];
        }
        System.out.printf("ClientId %s%n", clientId);
        ActiveMQConnectionFactory connFact = new ActiveMQConnectionFactory("tcp://localhost:61616");
        connFact.setConnectResponseTimeout(10000);
        Connection conn = connFact.createConnection("admin", "admin");
        conn.setClientID(clientId);
        conn.start();

        int howManyToSend = 3;
        if (args.length >= 2) {
            try {
                howManyToSend = Integer.valueOf(args[1]);
            } catch (NumberFormatException nfe) {
                usageExit("Number of messages to send, not a valid number : " + args[1]);
            }
        }



        send(conn, howManyToSend);
        receive(conn);
        // leaving this running forever so that we can stop and start responder
        // could timeout and shutdown
    }


    private static void usageExit(String message) {
        System.out.println(message);
        System.out.println("Argument 0: a client id, default: Requester");
        System.out.println("Argument 1: a number of messages to send, 0 for none, default 5");
            }

        private static void receive(Connection conn) throws JMSException{
        Session session = conn.createSession(false, Session.CLIENT_ACKNOWLEDGE);
        MessageConsumer consumer = session.createConsumer(session.createQueue(responseQueue));
        consumer.setMessageListener(new MessageListener() {
            @Override
            public void onMessage(Message message) {
                try {
                    String payload = ((TextMessage) message).getText();
                    String correlationId = message.getJMSCorrelationID();
                    System.out.println(String.format("Received message '%s' with correlation id '%s'", payload, correlationId));

                    Map<String, String> originalRequest = sentRequests.get(correlationId);
                    if (originalRequest == null){
                        System.out.printf("discarding unexcpeted response %s%n",correlationId);
                    } else {
                        System.out.printf("response for %s%n",originalRequest.get("book") );
                    }
                    // message successfully processed
                    message.acknowledge();
                } catch (Exception e) {
                    System.out.println("Exception " + e);
                    //e.printStackTrace();
                }
            }
        });
    }

    private static void send(Connection conn, int howManyToSend) throws JMSException {

        System.out.printf("Sending %d messages%n", howManyToSend);

        Session session = conn.createSession(false, Session.AUTO_ACKNOWLEDGE);

        String requestQueue = "bookingRequest";

        MessageProducer messageProducer = session.createProducer(session.createQueue(requestQueue));
        messageProducer.setPriority(3);
        messageProducer.setDeliveryMode(DeliveryMode.NON_PERSISTENT);



        for (long counter = 0; counter < howManyToSend; counter++) {

            // payload data as a HashMap
            Map<String, String> payloadMap = new HashMap<>();
            payloadMap.put("user", "DaveA");
            payloadMap.put("book", "room 10" + counter);

            // example date, really we'd ask the user when they want the room
            payloadMap.put("date", Instant.now().toString());
            Gson gson = new Gson();
            String payloadJson = gson.toJson(payloadMap);

            TextMessage message = session.createTextMessage(payloadJson);
            String msgId = UUID.randomUUID().toString();
            message.setJMSCorrelationID(msgId);

            Destination replyTo = session.createQueue(responseQueue);
            message.setJMSReplyTo(replyTo);

            messageProducer.send(message);
            sentRequests.put(msgId, payloadMap);
            System.out.printf("Sent %d: %s%n", counter, message);
        }

        messageProducer.close();
    }

}
