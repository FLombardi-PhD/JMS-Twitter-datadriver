package storm.twitter.datadriver;

import java.util.Properties;

import javax.jms.Connection;
import javax.jms.ConnectionFactory;
import javax.jms.MessageConsumer;
import javax.jms.Queue;
import javax.jms.Session;
import javax.jms.TextMessage;
import javax.naming.InitialContext;

import org.apache.log4j.Logger;
import org.apache.log4j.PropertyConfigurator;

public class TwitterDataConsumer {

	public static void main(String[] args) throws Exception {
		
		PropertyConfigurator.configure("data-consumer-log4j.properties");
		Logger logger = Logger.getLogger(TwitterDataConsumer.class);
		
		Properties props = new Properties();
		props.put("java.naming.factory.initial","org.jnp.interfaces.NamingContextFactory");
		props.put("java.naming.provider.url", args[0]);
		props.put("java.naming.factory.url.pkgs","org.jboss.naming:org.jnp.interfaces");
		InitialContext context = new InitialContext(props);
		logger.info("Context created!");
		
		Queue queue = (Queue)context.lookup("/queue/tweetQueue");
		logger.info("Queue looked up!");
		
		ConnectionFactory cf = (ConnectionFactory)context.lookup("/ConnectionFactory");
		logger.info("Connection Factory looked up!");
		
		Connection connection = cf.createConnection();
		logger.info("Connection created!");
		
		Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
		logger.info("Session created!");
		
		MessageConsumer messageConsumer = session.createConsumer(queue);
		logger.info("MessageConsumer created!");
		
		connection.start();
		logger.info("Connection started!");
		
		while (true) {
			TextMessage messageReceived = (TextMessage)messageConsumer.receive();
			logger.info("Received message: " + messageReceived.getText());
		}
	}

}
