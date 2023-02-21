import com.google.gson.Gson;
import org.apache.activemq.ActiveMQConnectionFactory;

import javax.jms.*;
import java.util.HashMap;
import java.util.Map;

// Service that receives a request message and sends a response to queue specified in the request header
// setting a correllation id in the response message
//
// command line arguments optional: clientId requestQueueName
//
public class Responder {
    private static Connection conn;
    private static Session session;
    public static void main(String[] args) throws Exception {
        String clientId = "Responder";
        if (args.length > 0){
            clientId = args[0];
        }
        System.out.printf("ClientId %s%n", clientId);
        ActiveMQConnectionFactory connFact = new ActiveMQConnectionFactory("tcp://localhost:61616");
        connFact.setConnectResponseTimeout(10000);
        conn = connFact.createConnection("admin", "admin");
        conn.setClientID(clientId);
        conn.start();
        Session session = conn.createSession(false, Session.CLIENT_ACKNOWLEDGE);

        // queue on which requests arrive
        String requestQueue = "bookingRequest";
        if (args.length > 1){
            requestQueue = args[1];
        }

        receive(conn, requestQueue);
    }

    /*
     * Receive requests and send responses
     */
    private static void receive(Connection conn, String requestQueue) throws JMSException{
        Session session = conn.createSession(false, Session.AUTO_ACKNOWLEDGE);
        MessageConsumer consumer = session.createConsumer(session.createQueue(requestQueue));
        consumer.setMessageListener(new MessageListener() {
            @Override
            public void onMessage(Message requestMessage) {
                try {
                    System.out.println(String.format("received message '%s' with message id '%s'", ((TextMessage) requestMessage).getText(), requestMessage.getJMSMessageID()));
                    // payload expected to be a JSON string, parse to a HashMap
                    String payload = ((TextMessage) requestMessage).getText();
                    Gson gson = new Gson();
                    Map<String, Object> map = gson.fromJson(payload, HashMap.class);

                    // TODO validate received message
                    // expect to have request to book a room for a user
                    String response = String.format("Request accepted: %s for %s",
                                         map.get("book"),
                                         map.get("user")
                                         );
                    // get reply information from request header
                    Destination replyQueue = requestMessage.getJMSReplyTo();
                    String correlationId = requestMessage.getJMSCorrelationID();
                    int priority = requestMessage.getJMSPriority();
                    int deliveryMode = requestMessage.getJMSDeliveryMode();

                    TextMessage responseMessage = session.createTextMessage(response);
                    responseMessage.setJMSCorrelationID(requestMessage.getJMSCorrelationID());

                    MessageProducer messageProducer = session.createProducer(replyQueue);
                    // priority and delivery mode to be same as request message
                    messageProducer.setPriority(priority);
                    messageProducer.setDeliveryMode(deliveryMode);

                    messageProducer.send(responseMessage);
                    System.out.println(String.format("sent response to '%s' ",
                                          replyQueue.toString()) );
                    
                } catch (JMSException e) {
                    e.printStackTrace();
                }
            }
        });
    }

}
