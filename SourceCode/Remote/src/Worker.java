

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import com.amazonaws.AmazonClientException;
import com.amazonaws.AmazonServiceException;
import com.amazonaws.auth.AWSCredentials;
import com.amazonaws.auth.profile.ProfileCredentialsProvider;
import com.amazonaws.regions.Region;
import com.amazonaws.regions.Regions;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDB;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDBClient;
import com.amazonaws.services.dynamodbv2.model.AttributeDefinition;
import com.amazonaws.services.dynamodbv2.model.AttributeValue;
import com.amazonaws.services.dynamodbv2.model.ComparisonOperator;
import com.amazonaws.services.dynamodbv2.model.Condition;
import com.amazonaws.services.dynamodbv2.model.CreateTableRequest;
import com.amazonaws.services.dynamodbv2.model.KeySchemaElement;
import com.amazonaws.services.dynamodbv2.model.KeyType;
import com.amazonaws.services.dynamodbv2.model.ProvisionedThroughput;
import com.amazonaws.services.dynamodbv2.model.PutItemRequest;
import com.amazonaws.services.dynamodbv2.model.PutItemResult;
import com.amazonaws.services.dynamodbv2.model.ScalarAttributeType;
import com.amazonaws.services.dynamodbv2.model.ScanRequest;
import com.amazonaws.services.dynamodbv2.model.ScanResult;
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

public class Worker {
	
		private static AmazonDynamoDBClient dynamoDB = null;

	public static void main(String[] args) throws Exception {
		 String Qname=args[0];
		 String noofworkers=args[1];
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
	        dynamoDB = new AmazonDynamoDBClient(credentials);
	        Region usWest2 = Region.getRegion(Regions.US_WEST_2);
	        sqs.setRegion(usWest2);
	        dynamoDB.setRegion(usWest2);

	        System.out.println("===========================================");
	        System.out.println("Getting Started with Amazon SQS");
	        System.out.println("===========================================\n");
	       
	        try {
	        	 CreateQueueRequest createQueueRequest = new CreateQueueRequest("ResltQueue");
 	            String myQueueUrl1 = sqs.createQueue(createQueueRequest).getQueueUrl();
 	            String tablename = "DynamoDBtable";
	     
 	           // Receive messages
	            System.out.println("Receiving messages from Queue.\n");
	            String myQueueUrl= 	"https://sqs.us-west-2.amazonaws.com/390984104847/ReqQueue";
	            long start = System.currentTimeMillis();
	            while(true)
	            {
        			try{
	            	
	            		ReceiveMessageRequest receiveMessageRequest = new ReceiveMessageRequest(myQueueUrl);
	            		
	    	            List<Message> messages = sqs.receiveMessage(receiveMessageRequest).getMessages();
	    	            for (Message message : messages) {
	    	                System.out.println("  Message");
	    	                System.out.println("    MessageId:     " + message.getMessageId());
	    	                System.out.println("    ReceiptHandle: " + message.getReceiptHandle());
	    	                System.out.println("    MD5OfBody:     " + message.getMD5OfBody());
	    	                System.out.println("    Body:          " + message.getBody());
	    	                String str=message.getBody();
	    	                String[] splitStr = str.split("\\s+");
	    	                String msgid= message.getMessageId();
	    	               
	    	                if (search(tablename,message.getMessageId()))
	    	                	System.out.println("Message ID Present!! ie no worker has taken  the work ");
	    	               
	    						else {
	    						System.out.println("Message ID Not present!! Add MsgId in DynamoDB and Execute!! ");
	    						try{
	    						Map<String, AttributeValue> item = newItem(msgid);
	    						PutItemRequest putItemRequest = new PutItemRequest(tablename, item);
	    						PutItemResult putItemResult = dynamoDB.putItem(putItemRequest);
	    						System.out.println("Result: " + putItemResult);
	    						}
	    						catch(Exception e)
	    						{
	    							System.out.println(e);
	    						}
	    					System.out.println("performing the task");
	    	                Thread.sleep(Integer.parseInt(splitStr[1]));
	    	                sqs.sendMessage(new SendMessageRequest(myQueueUrl1, message.getBody()));
	    	                for (Entry<String, String> entry : message.getAttributes().entrySet()) {
	    	                    System.out.println("  Attribute");
	    	                    System.out.println("    Name:  " + entry.getKey());
	    	                    System.out.println("    Value: " + entry.getValue());
	    	                }
	    	            }
	    	            }
	    	            
	    	            // Delete a message
	    	            System.out.println("Deleting a message.\n");
	    	            String messageReceiptHandle = messages.get(0).getReceiptHandle();
	    	            sqs.deleteMessage(new DeleteMessageRequest(myQueueUrl, messageReceiptHandle));
	    	            }
        			
	            	catch(Exception e)
	            	{
	            		break;
	            	}
	            }
	            System.out.println("Time"+((System.currentTimeMillis() - start)/1000));
				
	        } catch (AmazonClientException ace) {
	            System.out.println("1Caught an AmazonClientException, which means the client encountered " +
	                    "a serious internal problem while trying to communicate with SQS, such as not " +
	                    "being able to access the network.");
	            System.out.println("Error Message: " + ace.getMessage());
	        }
	    }

	private static Map<String, AttributeValue> newItem(String para1) {
		System.out.println("para1"+para1);
		Map<String, AttributeValue> item = new HashMap<String, AttributeValue>();
		item.put("MsgId", new AttributeValue(para1));
		return item;
	}
	public static boolean search(String tableName, String in_para) {

		// Searches parameter in dynamoDB
		HashMap<String, Condition> scanFilter = new HashMap<String, Condition>();
		Condition condition = new Condition().withComparisonOperator(
				ComparisonOperator.EQ.toString()).withAttributeValueList(
				new AttributeValue().withS(in_para));
		scanFilter.put("MsgId", condition);
		
		ScanRequest scanRequest = new ScanRequest(tableName)
				.withScanFilter(scanFilter);
		ScanResult scanResult = dynamoDB.scan(scanRequest);
		System.out.println("Result: " + scanResult);
		if (scanResult.getCount() == 0)
			return false;
		else
			return true;

	}
	
	
}
	

