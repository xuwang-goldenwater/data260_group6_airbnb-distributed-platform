// Run using mongosh < db/mongo/init.js, or make platform-init (same indexes in Python).
db = db.getSiblingDB('airbnb');
db.conversations.createIndex({conversation_id: 1}, {unique: true});
db.conversations.createIndex({reservation_id: 1}, {unique: true});
db.messages.createIndex({message_id: 1}, {unique: true});
db.messages.createIndex({sender_id: 1, http_key: 1}, {unique: true, partialFilterExpression: {http_key: {$type: 'string'}}});
db.messages.createIndex({conversation_id: 1, created_datetime: 1, message_id: 1});
db.messages.createIndex({publish_pending: 1});
db.reviews.createIndex({review_id: 1}, {unique: true});
db.reviews.createIndex({reservation_id: 1}, {unique: true});
db.reviews.createIndex({property_id: 1, created_datetime: -1});
db.event_traces.createIndex({event_id: 1}, {unique: true});
db.event_traces.createIndex({idempotency_key: 1}, {unique: true});
db.event_traces.createIndex({'payload.property_id': 1, timestamp: 1});
db.event_traces.createIndex({trace_id: 1});
db.debates.createIndex({trace_id: 1}, {unique: true});
