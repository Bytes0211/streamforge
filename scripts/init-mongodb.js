// MongoDB Initialization Script for StreamForge
// Run with: docker exec -i streamforge-mongodb mongosh -u admin -p password --authenticationDatabase admin < scripts/init-mongodb.js

print("=== StreamForge MongoDB Initialization ===");

// Switch to streamforge database
db = db.getSiblingDB('streamforge');

print("Creating database: streamforge");

// Create processed_data collection with validation
db.createCollection("processed_data", {
  validator: {
    $jsonSchema: {
      bsonType: "object",
      required: ["data", "timestamp", "processedAt"],
      properties: {
        data: {
          bsonType: "string",
          description: "Processed data payload - must be a string"
        },
        timestamp: {
          bsonType: "long",
          description: "Processing timestamp in epoch milliseconds - must be a number"
        },
        processedAt: {
          bsonType: "date",
          description: "Processing datetime - must be a date"
        },
        sourceOffset: {
          bsonType: "long",
          description: "Optional Kafka offset for tracking"
        },
        partition: {
          bsonType: "int",
          description: "Optional Kafka partition for tracking"
        }
      }
    }
  },
  validationLevel: "moderate",
  validationAction: "warn"
});

print("Created collection: processed_data with validation rules");

// Create indexes
db.processed_data.createIndex({ timestamp: -1 }, { name: "idx_timestamp" });
print("Created index: idx_timestamp (descending)");

db.processed_data.createIndex({ processedAt: -1 }, { name: "idx_processedAt" });
print("Created index: idx_processedAt (descending)");

// Insert a sample document for testing
db.processed_data.insertOne({
  data: "SAMPLE TEST DATA",
  timestamp: NumberLong(Date.now()),
  processedAt: new Date()
});

print("Inserted sample document for testing");

// Display collection stats
print("\n=== Collection Stats ===");
print("Collection: processed_data");
print("Document count: " + db.processed_data.countDocuments());
print("Indexes: " + db.processed_data.getIndexes().length);

print("\n=== Indexes ===");
db.processed_data.getIndexes().forEach(function(index) {
  print("  - " + index.name + ": " + JSON.stringify(index.key));
});

print("\n=== Sample Document ===");
printjson(db.processed_data.findOne());

print("\n=== Initialization Complete ===");
