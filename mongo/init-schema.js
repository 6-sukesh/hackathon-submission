// Primary DB schema. Run: docker exec -i mongo mongosh --quiet < mongo/init-schema.js
// Validators list required fields only; extra fields are allowed so producers can evolve the schema.
// Only entities live here (users, tracks). Plays and clicks are published to Kafka, not stored in Mongo:
// see contracts/events.md.
db = db.getSiblingDB("music");

function ensure(name, schema, indexes) {
  if (!db.getCollectionNames().includes(name)) db.createCollection(name);
  db.runCommand({ collMod: name, validator: { $jsonSchema: schema }, validationLevel: "moderate" });
  indexes.forEach(([keys, opts]) => db[name].createIndex(keys, opts || {}));
}

ensure("users", {
  bsonType: "object",
  required: ["user_id", "tenant_id", "display_name", "created_at"],
  properties: {
    user_id: { bsonType: "string" },
    tenant_id: { bsonType: "string" },
    display_name: { bsonType: "string" },
    email: { bsonType: "string" },
    country: { bsonType: "string" },
    plan: { enum: ["free", "premium"] },
    created_at: { bsonType: "date" },
    updated_at: { bsonType: "date" },
  },
}, [[{ user_id: 1 }, { unique: true }], [{ tenant_id: 1 }]]);

ensure("tracks", {
  bsonType: "object",
  required: ["track_id", "track_name", "artist_name", "duration_ms"],
  properties: {
    track_id: { bsonType: "string" },
    track_name: { bsonType: "string" },
    artist_id: { bsonType: "string" },
    artist_name: { bsonType: "string" },
    album_name: { bsonType: "string" },
    genre: { bsonType: "string" },
    duration_ms: { bsonType: "int" },
    popularity: { bsonType: "int" },
    release_year: { bsonType: "int" },
    danceability: { bsonType: ["double", "int"] },
    energy: { bsonType: ["double", "int"] },
    valence: { bsonType: ["double", "int"] },
    tempo: { bsonType: ["double", "int"] },
    acousticness: { bsonType: ["double", "int"] },
    updated_at: { bsonType: "date" },
  },
}, [[{ track_id: 1 }, { unique: true }], [{ genre: 1 }]]);

print("music schema ready: " + db.getCollectionNames().sort().join(", "));
