package com.reviews.analysis.response;

import com.amazonaws.services.dynamodbv2.AmazonDynamoDB;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDBClientBuilder;
import com.amazonaws.services.dynamodbv2.document.DynamoDB;
import com.amazonaws.services.dynamodbv2.document.Table;
import com.amazonaws.services.dynamodbv2.document.spec.QuerySpec;
import com.amazonaws.services.dynamodbv2.document.Item;
import com.amazonaws.services.lambda.runtime.Context;
import com.amazonaws.services.lambda.runtime.RequestHandler;

import java.util.HashMap;
import java.util.Map;

public class ReadReviewAnalysisLambda implements RequestHandler<Map<String, String>, Map<String, Object>> {

    private static final String ANALYSIS_TABLE = "ProductReviewAnalysis";
    private DynamoDB dynamoDB;

    @Override
    public Map<String, Object> handleRequest(Map<String, String> input, Context context) {
        initializeDynamoDB();

        String productId = input.get("product_id");
        if (productId == null || productId.isEmpty()) {
            return Map.of("result", "Error: product_id is missing.");
        }

        Map<String, Object> reviewAnalysis = fetchReviewAnalysis(productId);
        if (reviewAnalysis == null) {
            return Map.of("result", "Product not found or no review analysis available.");
        }

        return reviewAnalysis;
    }

    private void initializeDynamoDB() {
        AmazonDynamoDB client = AmazonDynamoDBClientBuilder.standard().build();
        dynamoDB = new DynamoDB(client);
    }

    private Map<String, Object> fetchReviewAnalysis(String productId) {
        Table table = dynamoDB.getTable(ANALYSIS_TABLE);

        // Query to find the item by product_id
        QuerySpec querySpec = new QuerySpec()
                .withKeyConditionExpression("product_id = :v_id")
                .withValueMap(Map.of(":v_id", productId));

        // Get the item from the table
        Item item = table.query(querySpec).iterator().hasNext() ? table.query(querySpec).iterator().next() : null;

        if (item == null) {
            return null; // No item found
        }

        // Fetch the review_analysis attribute
        Map<String, Object> reviewAnalysis = item.getMap("review_analysis");
        return reviewAnalysis != null ? reviewAnalysis : new HashMap<>();
    }
}

