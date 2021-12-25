"use strict";

const { databaseNew, databaseCreateCollection, collectionInsertOne, databaseGetById } = require("./index.node");

class Database {
    constructor(path) {
        this.db = databaseNew(path);
    }

    createCollection(name) {
        let binded = databaseCreateCollection.bind(this.db);
        return new Collection(binded(name));
    }
}

class Collection {
    constructor(collection) {
        this.collection = collection;
    }

    insertOne(data) {
        let binded = collectionInsertOne.bind(this.collection);
        binded(data);
    }
}

module.exports = Database;