"use strict";

const { databaseNew, databaseCreateCollection, databaseInsert, databaseGetById } = require("./index.node");

class Database {
    constructor(path) {
        this.db = databaseNew(path);
    }

    createCollection(name) {
        let binded = databaseCreateCollection.bind(this.db);
        binded(name);
    }
}

module.exports = Database;