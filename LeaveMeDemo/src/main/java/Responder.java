import com.google.gson.Gson;
import org.apache.activemq.ActiveMQConnectionFactory;

import javax.jms.*;
import java.util.HashMap;
import java.util.Map;

// example of a service that receives a request message
// and sends a response to queue specified in the request header
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

    private static void usageExit(String message) {
        System.out.println(message);
        System.out.println("Argument 0, optional: a client id, default Responder");
        System.out.println("Argument 1, optional: request queue name, default bookingRequest");
    }

    private static void receive(Connection conn, String requestQueue) throws JMSException{
        Session session = conn.createSession(false, Session.CLIENT_ACKNOWLEDGE);
        MessageConsumer consumer = session.createConsumer(session.createQueue(requestQueue));
        consumer.setMessageListener(new MessageListener() {
            @Override
            public void onMessage(Message message) {
                try {
                    System.out.println(String.format("received message '%s' with message id '%s'", ((TextMessage) message).getText(), message.getJMSMessageID()));
                    String payload = null;
                    payload = ((TextMessage) message).getText();
                    Gson gson = new Gson();
                    Map map = gson.fromJson(payload, HashMap.class);
                    System.out.println(String.format("User %s", map.get("user") ) );

                    Destination replyQueue = message.getJMSReplyTo();
                    String correlationId = message.getJMSCorrelationID();

                    String response = String.format("Request accepted: %s for %s",
                                         map.get("book"),
                                         map.get("user")
                                         );
                    TextMessage responseMessage = session.createTextMessage(response);
                    responseMessage.setJMSCorrelationID(message.getJMSCorrelationID());

                    MessageProducer messageProducer = session.createProducer(replyQueue);
                    // TODO adjust priority and delivery mode to be same as request message

                    messageProducer.setPriority(3);
                    messageProducer.setDeliveryMode(DeliveryMode.NON_PERSISTENT);
                    messageProducer.send(responseMessage);
                    System.out.println(String.format("sent response to '%s' ",
                                          replyQueue.toString()) );

                    // we have processed the message
                    message.acknowledge();


                } catch (JMSException e) {
                    e.printStackTrace();
                }
            }
        });
    }

}
