package com.malsm.a2app;

import com.amazonaws.services.dynamodbv2.AmazonDynamoDB;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDBClientBuilder;
import com.amazonaws.services.dynamodbv2.document.DynamoDB;
import com.amazonaws.services.dynamodbv2.document.Table;
import com.amazonaws.services.dynamodbv2.document.Item;
import com.amazonaws.services.lambda.runtime.Context;
import com.amazonaws.services.lambda.runtime.RequestHandler;
import com.amazonaws.services.lambda.runtime.events.S3Event;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import com.amazonaws.services.s3.model.S3Object;

import java.io.BufferedReader;
import java.io.FileWriter;
import java.io.InputStreamReader;
import java.util.UUID;
import org.json.JSONArray;
import org.json.JSONObject;

public class Maslama2App implements RequestHandler<S3Event, String> {

    private DynamoDB dynamoDB;
    private String DYNAMODB_TABLE_NAME = "ProductReview";
    private String LOG_FILE_PATH = "/tmp/upload_log.txt"; // Temp directory for AWS Lambda

    public Maslama2App() {
        // Initialize DynamoDB client
        AmazonDynamoDB client = AmazonDynamoDBClientBuilder.standard().build();
        this.dynamoDB = new DynamoDB(client);
    }

    @Override
    public String handleRequest(S3Event event, Context context) {
        // Get bucket name and object key from the event
        String bucketName = event.getRecords().get(0).getS3().getBucket().getName();
        String objectKey = event.getRecords().get(0).getS3().getObject().getKey();
        
        context.getLogger().log("Bucket: " + bucketName + " Key: " + objectKey);

        // Log the bucket and file names
        logBucketAndFileName(bucketName, objectKey, context);

        // Get the S3 object
        AmazonS3 s3Client = AmazonS3ClientBuilder.standard().build();
        S3Object s3Object = s3Client.getObject(bucketName, objectKey);

        try (BufferedReader reader = new BufferedReader(new InputStreamReader(s3Object.getObjectContent()))) {
            String line;
            StringBuilder fileContent = new StringBuilder();
            // Read the file content
            while ((line = reader.readLine()) != null) {
                fileContent.append(line);
            }
            // Process the file content based on its format
            processFileContent(fileContent.toString(), context);
        } catch (Exception e) {
            context.getLogger().log("Error reading S3 object: " + e.getMessage());
        }
        
        return "Processing Done";
    }

    private void logBucketAndFileName(String bucketName, String objectKey, Context context) {
        try (FileWriter writer = new FileWriter(LOG_FILE_PATH, true)) {
            writer.write("Bucket: " + bucketName + ", Key: " + objectKey + "\n");
        } catch (Exception e) {
            context.getLogger().log("Error logging bucket and file name: " + e.getMessage());
        }
    }

    private void processFileContent(String content, Context context) {
        // Determine if the content is JSON or TXT format
        if (content.trim().startsWith("[")) {
            processJsonContent(content, context);
        } else {
            processTxtContent(content, context);
        }
    }

    private void processJsonContent(String content, Context context) {
        // Parse JSON content and insert records into DynamoDB
        JSONArray jsonArray = new JSONArray(content);
        for (int i = 0; i < jsonArray.length(); i++) {
            JSONObject jsonObject = jsonArray.getJSONObject(i);
            try {
                String productName = jsonObject.getString("ProductName");
                double price = jsonObject.getDouble("Price");
                String review = jsonObject.getString("Review");
                double rating = jsonObject.getDouble("Rating");
                insertRecord(productName, price, review, rating);
                context.getLogger().log("Inserted JSON record: " + jsonObject.toString());
            } catch (Exception e) {
                context.getLogger().log("Error processing JSON record: " + e.getMessage());
                context.getLogger().log("Record: " + jsonObject.toString());
            }
        }
    }

   private void processTxtContent(String content, Context context) {
    // Parse TXT content and insert records into DynamoDB
    String[] records = content.split(";");
    for (String record : records) {
        try {
            // Split each record by ", " to handle the fields
            String[] attributes = record.split(", (?=(?:[^\"]*\"[^\"]*\")*[^\"]*$)");
            
            String productName = null;
            double price = 0;
            StringBuilder review = new StringBuilder();
            double rating = 0;

            for (String attribute : attributes) {
                String[] keyValue = attribute.split(":");
                if (keyValue.length < 2) {
                    continue; // skip malformed attribute
                }
                String key = keyValue[0].trim();
                String value = keyValue[1].trim();
                
                switch (key) {
                    case "ProductName":
                        productName = value;
                        break;
                    case "Price":
                        price = Double.parseDouble(value);
                        break;
                    case "Review":
                        review.append(value);
                        break;
                    case "Rating":
                        rating = Double.parseDouble(value);
                        break;
                    default:
                        // For handling commas in review
                        review.append(", ").append(key).append(":").append(value);
                }
                context.getLogger().log("Attribute: " + attribute);
            }

            if (productName != null && review.length() > 0) {
                insertRecord(productName, price, review.toString(), rating);
                context.getLogger().log("Inserted TXT record: " + record);
            } else {
                context.getLogger().log("Error processing TXT record: empty String");
            }
        } catch (Exception e) {
            context.getLogger().log("Error processing TXT record: " + e.getMessage());
            context.getLogger().log("Record: " + record);
        }
    }
}


    private String getValue(String attribute) {
        try {
            return attribute.split(":")[1].trim();
        } catch (Exception e) {
            return "";
        }
    }

    private void insertRecord(String productName, double price, String review, double rating) {
        // Insert record into DynamoDB table
        Table table = dynamoDB.getTable(DYNAMODB_TABLE_NAME);
        Item item = new Item()
                .withPrimaryKey("Identifier", UUID.randomUUID().toString())
                .withString("ProductName", productName)
                .withNumber("Price", price)
                .withString("Review", review)
                .withNumber("Rating", rating);
        table.putItem(item);
    }
}
