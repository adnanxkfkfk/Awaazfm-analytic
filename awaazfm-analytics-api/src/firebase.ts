// Helper for Firebase REST API interactions

export interface FirestoreDocument {
  fields: Record<string, any>;
}

export function createFirestoreDocument(event: any): FirestoreDocument {
  return {
    fields: {
      type: { stringValue: event.type },
      timestamp: { integerValue: event.timestamp.toString() }, // Firestore uses string for large ints
      session_id: { stringValue: event.session_id },
      payload: { stringValue: JSON.stringify(event.payload) } // Storing complex payload as JSON string to simplify schema
    }
  };
}

// In a real implementation, you would construct a batchWrite payload:
// POST https://firestore.googleapis.com/v1/projects/{projectId}/databases/{databaseId}/documents:commit
export function buildBatchWritePayload(events: any[], collection: string) {
    const writes = events.map(evt => ({
        update: {
            name: `${collection}/${evt.session_id}_${evt.timestamp}`, // Document ID strategy
            fields: createFirestoreDocument(evt).fields
        }
    }));
    
    return {
        writes
    };
}
