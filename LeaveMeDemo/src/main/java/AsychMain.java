import org.apache.activemq.ActiveMQConnectionFactory;

import javax.jms.Connection;
import javax.jms.Session;

public class AsychMain {
    public static void main(String[] args) throws Exception {
        String clientId = "Example";
        if (args.length > 0){
            clientId = args[0];
        }
        System.out.printf("ClientId %s%n", clientId);
        ActiveMQConnectionFactory connFact = new ActiveMQConnectionFactory("tcp://localhost:61616");
        connFact.setConnectResponseTimeout(10000);
        Connection conn = connFact.createConnection("admin", "admin");
        conn.setClientID(clientId);
        conn.start();

        int howManyToSend = 5;
        boolean receive = true;

        if (args.length < 2) {
            howManyToSend = 5;
            receive = true;
        }
        if (args.length >= 2) {
            try {
                howManyToSend = Integer.valueOf(args[1]);
            } catch (NumberFormatException nfe) {
                usageExit("Number of messages to send, not a valid number : " + args[1]);
            }
        }
        if (args.length >= 3) {
            receive = ("receive".equalsIgnoreCase(args[2]));
        }

        //String destination = "Queue.PointToPoint.OneWay.Traditional";
        String destination = "audit";

        if (howManyToSend > 0) {
            System.out.printf("Sending %d messages%n", howManyToSend);
            new Thread(new Sender(
                    conn.createSession(false, Session.CLIENT_ACKNOWLEDGE),
                    destination,
                    howManyToSend)
                  ).start();

        }
        if ( receive) {
            System.out.printf("Receiving messages%n");
            new Thread(new Receiver(conn.createSession(false, Session.CLIENT_ACKNOWLEDGE),
                    destination)).start();
        }
    }

    private static void usageExit(String message) {
        System.out.println(message);
        System.out.println("Argument 1: a number of messages to send, 0 for none, default 5");
        System.out.println("Optional second argument: 'receive' will start a receiver, other values will not, default to receive");

    }

}
