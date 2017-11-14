

import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.FileReader;
import java.io.InputStreamReader;
import java.util.List;
import java.util.Map.Entry;

import com.amazonaws.AmazonClientException;
import com.amazonaws.AmazonServiceException;
import com.amazonaws.auth.AWSCredentials;
import com.amazonaws.auth.profile.ProfileCredentialsProvider;
import com.amazonaws.regions.Region;
import com.amazonaws.regions.Regions;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDBClient;
import com.amazonaws.services.dynamodbv2.model.AttributeDefinition;
import com.amazonaws.services.dynamodbv2.model.CreateTableRequest;
import com.amazonaws.services.dynamodbv2.model.KeySchemaElement;
import com.amazonaws.services.dynamodbv2.model.KeyType;
import com.amazonaws.services.dynamodbv2.model.ProvisionedThroughput;
import com.amazonaws.services.dynamodbv2.model.ScalarAttributeType;
import com.amazonaws.services.dynamodbv2.model.TableDescription;
import com.amazonaws.services.dynamodbv2.util.Tables;
import com.amazonaws.services.sqs.AmazonSQS;
import com.amazonaws.services.sqs.AmazonSQSClient;
import com.amazonaws.services.sqs.model.CreateQueueRequest;
import com.amazonaws.services.sqs.model.DeleteMessageRequest;
import com.amazonaws.services.sqs.model.DeleteQueueRequest;
import com.amazonaws.services.sqs.model.Message;
import com.amazonaws.services.sqs.model.ReceiveMessageRequest;
import com.amazonaws.services.sqs.model.SendMessageRequest;

public class Client {
	 public static String tableName;
	
	 public static void main(String[] args) throws Exception {
		 String Qname=args[0];
		 String fname=args[1];
		 FileInputStream fstream = new FileInputStream(fname);
		 BufferedReader br = new BufferedReader(new InputStreamReader(fstream));
		 String strLine;
			
		 	/*
	         * The ProfileCredentialsProvider will return your [default]
	         * credential profile by reading from the credentials file located at
	         * (/home/biya/.aws/credentials).
	         */
	        AWSCredentials credentials = null;
	        try {
	            credentials = new ProfileCredentialsProvider("default").getCredentials();
	        } catch (Exception e) {
	            throw new AmazonClientException(
	                    "Cannot load the credentials from the credential profiles file. " +
	                    "Please make sure that your credentials file is at the correct " +
	                    "location (/home/biya/.aws/credentials), and is in valid format.",
	                    e);
	        }

	        AmazonSQS sqs = new AmazonSQSClient(credentials);
	        AmazonDynamoDBClient dynamoDB = new AmazonDynamoDBClient(credentials);
	        Region usWest2 = Region.getRegion(Regions.US_WEST_2);
	        sqs.setRegion(usWest2);
	        dynamoDB.setRegion(usWest2);

	        System.out.println("===========================================");
	        System.out.println("Getting Started with Amazon SQS");
	        System.out.println("===========================================\n");

	        try {
	        	
	        	   // Create a queue
	            System.out.println("Creating a new SQS queue.\n");
	            CreateQueueRequest createQueueRequest = new CreateQueueRequest(Qname);
	            String myQueueUrl = sqs.createQueue(createQueueRequest).getQueueUrl();
	            
	            //create a table
	        	
	        	tableName = "DynamoDBtable";
	        	// Create table if it does not exist yet
	 	            if (Tables.doesTableExist(dynamoDB, tableName)) {
	 	                System.out.println("Table " + tableName + " is already ACTIVE");
	 	            } else {
	 	                // Create a table with a primary hash key named 'MsgId', which holds messageId
	 	                CreateTableRequest createTableRequest = new CreateTableRequest().withTableName(tableName)
	 	                    .withKeySchema(new KeySchemaElement().withAttributeName("MsgId").withKeyType(KeyType.HASH))
	 	                    .withAttributeDefinitions(new AttributeDefinition().withAttributeName("MsgId").withAttributeType(ScalarAttributeType.S))
	 	                    .withProvisionedThroughput(new ProvisionedThroughput().withReadCapacityUnits(1L).withWriteCapacityUnits(1L));
	 	                    TableDescription createdTableDescription = dynamoDB.createTable(createTableRequest).getTableDescription();
	 	                System.out.println("Created Table: " + createdTableDescription);

	 	                // Wait for it to become active
	 	                System.out.println("Waiting for " + tableName + " to become ACTIVE...");
	 	                Tables.awaitTableToBecomeActive(dynamoDB, tableName);
	 	            }
	         
	            
	            // Send a message
	            System.out.println("Sending a message to Queue.\n");
	            while ((strLine = br.readLine()) != null)   {
	            sqs.sendMessage(new SendMessageRequest(myQueueUrl, strLine));
	            }
	            
	        } catch (AmazonServiceException ase) {
	            System.out.println("Caught an AmazonServiceException, which means your request made it " +
	                    "to Amazon SQS, but was rejected with an error response for some reason.");
	            System.out.println("Error Message:    " + ase.getMessage());
	            System.out.println("HTTP Status Code: " + ase.getStatusCode());
	            System.out.println("AWS Error Code:   " + ase.getErrorCode());
	            System.out.println("Error Type:       " + ase.getErrorType());
	            System.out.println("Request ID:       " + ase.getRequestId());
	        } catch (AmazonClientException ace) {
	            System.out.println("Caught an AmazonClientException, which means the client encountered " +
	                    "a serious internal problem while trying to communicate with SQS, such as not " +
	                    "being able to access the network.");
	            System.out.println("Error Message: " + ace.getMessage());
	        }
	 }   
}
