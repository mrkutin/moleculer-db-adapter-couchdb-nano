/*
 * moleculer-db-adapter-couchdb-nano
 * Copyright (c) 2018 Mr. Kutin (https://github.com/moleculerjs/moleculer-db)
 * MIT Licensed
 */

"use strict";

const _ = require("lodash");
const Promise = require("bluebird");

class CouchDbNanoAdapter {

    /**
     * Creates an instance of CouchDbNano.
     * @param {String} uri
     * @param {Object?} opts
     *
     * @memberof CouchDbNano
     */
    constructor(uri, opts) {
        this.uri = uri;
        this.opts = opts;
        this.db = null;
    }

    /**
     * Initialize adapter
     *
     * @param {ServiceBroker} broker
     * @param {Service} service
     *
     * @memberof CouchDbNano
     */
    init(broker, service) {
        this.broker = broker;
        this.service = service;

        if (!this.service.schema.collection) {
            /* istanbul ignore next */
            throw new Error("Missing `collection` definition in schema of service!");
        }
    }

    /**
     * Connect to database
     *
     * @returns {Promise}
     *
     * @memberof CouchDbNano
     */
    connect() {
        const opts = Object.assign({}, this.opts, {url: this.url ? this.uri.replace('couchdb://', 'http://') : '' || 'http://localhost:5984'});
        const nano = require('nano')(opts);
        const dbName = `${this.service.schema.name}-${this.service.schema.collection}`;

        return Promise.resolve(
            nano.db.get(dbName)
                .catch(e => {
                    if (e.statusCode === 404) {
                        return nano.db.create(dbName);
                    }
                    throw(e);
                })
                .then(() => this.db = nano.db.use(dbName))
        );
    }

    /**
     * Disconnect from database
     *
     * @returns {Promise}
     *
     * @memberof CouchDbNano
     */
    disconnect() {
        this.db = null;
        return Promise.resolve();
    }

    /**
     * Find all entities by filters.
     *
     * Available filter props:
     * - selector (json) – JSON object describing criteria used to select documents. More information provided in the section on selector syntax. Required
     * - limit (number) – Maximum number of results returned.
     * - skip (number) – Skip the first ‘n’ results, where ‘n’ is the value specified. Optional
     * - sort (json) – JSON array following sort syntax. Optional
     * - fields (array) – JSON array specifying which fields of each object should be returned. If it is omitted, the entire object is returned. More information provided in the section on filtering fields. Optional
     *
     * @param {Object} filters
     * @returns {Promise<Array>}
     *
     * @memberof CouchDbNano
     */
    find(filters) {
        const {selector = filters, limit, skip, sort, fields} = filters;
        Object.entries(selector).forEach(([key, value]) => {
            if(typeof value !== 'object'){
                selector[key] = {$eq: value};
            }
        });
        return Promise.resolve(this.db.find({selector, limit, skip, sort, fields}).then(result => result.docs));
    }

    /**
     * Find an entity by query
     *
     * Available filter props:
     * - selector (json) – JSON object describing criteria used to select documents. More information provided in the section on selector syntax. Required
     * - limit (number) – Maximum number of results returned. Default is 25. Optional
     * - skip (number) – Skip the first ‘n’ results, where ‘n’ is the value specified. Optional
     * - sort (json) – JSON array following sort syntax. Optional
     * - fields (array) – JSON array specifying which fields of each object should be returned. If it is omitted, the entire object is returned. More information provided in the section on filtering fields. Optional
     *
     * @param {Object} query
     * @returns {Promise}
     * @memberof MemoryDbAdapter
     */
    findOne(query) {
        const filters = Object.assign({}, {selector: query, limit: 1});
        return this.find(filters).then(docs => docs.length ? docs[0] : null);
    }

    /**
     * Find an entity by ID.
     *
     * @param {String} _id
     * @returns {Promise<Object>} Return with the found document.
     *
     * @memberof CouchDbNano
     */
    findById(_id) {
        return Promise.resolve(this.db.get(_id));
    }

    /**
     * Find any entities by IDs.
     *
     * @param {Array} idList
     * @returns {Promise<Array>} Return with the found documents in an Array.
     *
     * @memberof CouchDbNano
     */
    findByIds(idList) {
        return Promise.resolve(this.db.fetch({keys: idList}));
    }

    /**
     * Get count of filtered entites.
     *
     * Available filter props:
     * - selector (json) – JSON object describing criteria used to select documents. More information provided in the section on selector syntax. Required
     * - limit (number) – Maximum number of results returned. Default is 25. Optional
     * - skip (number) – Skip the first ‘n’ results, where ‘n’ is the value specified. Optional
     * - sort (json) – JSON array following sort syntax. Optional
     *
     * @param {Object} [filters={}]
     * @returns {Promise<Number>} Return with the count of documents.
     *
     * @memberof CouchDbNano
     */
    count(filters = {}) {
        return this.find(filters).then(docs => docs.length);
    }

    /**
     * Insert an entity.
     *
     * @param {Object} entity
     * @returns {Promise<Object>} Return with the inserted document.
     *
     * @memberof CouchDbNano
     */
    insert(entity) {
        return this.db.insert(entity).then(result => this.findById(result.id));
    }

    /**
     * Insert many entities
     *
     * @param {Array} entities
     * @returns {Promise<Array<Object>>} Return with the inserted documents in an Array.
     *
     * @memberof CouchDbNano
     */
    insertMany(entities) {
        return Promise.resolve(this.db.bulk(entities).then(result => result));
    }

    /**
     * Update many entities by `query` and `update`
     *
     * @param {Object} query
     * @param {Object} update
     * @returns {Promise<Number>} Return with the count of modified documents.
     *
     * @memberof CouchDbNano
     */
    updateMany(query, update) {
        return Promise.resolve(this.db.bulk(entities).then(result => result));
    }

    /**
     * Update an entity by ID and `update`
     *
     * @param {String} _id - ObjectID as hexadecimal string.
     * @param {Object} update
     * @returns {Promise<Object>} Return with the updated document.
     *
     * @memberof CouchDbNano
     */
    updateById(_id, update) {
        return this.findById(_id)
            .then(doc => Object.assign({}, doc, update))
            .then(mergedDoc => this.db.insert(mergedDoc))
            .then(result => result);
    }

    /**
     * Remove entities which are matched by `query`
     *
     * @param {Object} query
     * @returns {Promise<Number>} Return with the count of deleted documents.
     *
     * @memberof CouchDbNano
     */
    removeMany(query) {
        //todo docs: docs, _deleted for each doc
        return this.collection.deleteMany(query).then(res => res.deletedCount);
    }

    /**
     * Remove an entity by ID
     *
     * @param {String} _id - ObjectID as hexadecimal string.
     * @returns {Promise<Object>} Return with the removed document.
     *
     * @memberof CouchDbNano
     */
    removeById(_id) {
        return this.findById(_id).then(doc => this.db.destroy(doc));
    }

    /**
     * Clear all entities from collection
     *
     * @returns {Promise}
     *
     * @memberof CouchDbNano
     */
    clear() {
        return this.removeMany({}).then(res => res.deletedCount);
    }

    // /**
    //  * Create a filtered cursor.
    //  *
    //  * Available filters in `params`:
    //  *  - search
    //  *    - sort
    //  *    - limit
    //  *    - offset
    //  *  - query
    //  *
    //  * @param {Object} params
    //  * @param {Boolean} isCounting
    //  * @returns {MongoCursor}
    //  */
    // createCursor(params, isCounting) {
    //     if (params) {
    //         let q;
    //         if (isCounting)
    //             q = this.collection.countDocuments(params.query);
    //         else
    //             q = this.collection.find(params.query);
    //         // Full-text search
    //         // More info: https://docs.mongodb.com/manual/reference/operator/query/text/
    //         if (_.isString(params.search) && params.search !== "") {
    //             q = this.collection.find(Object.assign(params.query || {}, {
    //                 $text: {
    //                     $search: params.search
    //                 }
    //             }));
    //             q.project({_score: {$meta: "textScore"}});
    //             q.sort({
    //                 _score: {
    //                     $meta: "textScore"
    //                 }
    //             });
    //         } else {
    //             // Sort
    //             if (params.sort && q.sort) {
    //                 let sort = this.transformSort(params.sort);
    //                 if (sort)
    //                     q.sort(sort);
    //             }
    //         }
    //
    //         // Offset
    //         if (_.isNumber(params.offset) && params.offset > 0)
    //             q.skip(params.offset);
    //
    //         // Limit
    //         if (_.isNumber(params.limit) && params.limit > 0)
    //             q.limit(params.limit);
    //
    //         return q;
    //     }
    //
    //     // If not params
    //     if (isCounting)
    //         return this.collection.countDocuments({});
    //     else
    //         return this.collection.find({});
    // }
    //
    // /**
    //  * Convert the `sort` param to a `sort` object to Mongo queries.
    //  *
    //  * @param {String|Array<String>|Object} paramSort
    //  * @returns {Object} Return with a sort object like `{ "votes": 1, "title": -1 }`
    //  * @memberof CouchDbNano
    //  */
    // transformSort(paramSort) {
    //     let sort = paramSort;
    //     if (_.isString(sort))
    //         sort = sort.replace(/,/, " ").split(" ");
    //
    //     if (Array.isArray(sort)) {
    //         let sortObj = {};
    //         sort.forEach(s => {
    //             if (s.startsWith("-"))
    //                 sortObj[s.slice(1)] = -1;
    //             else
    //                 sortObj[s] = 1;
    //         });
    //         return sortObj;
    //     }
    //
    //     return sort;
    // }
    //
    /**
     * Transforms 'idField' into CouchDB's '_id'
     * @param {Object} entity
     * @param {String} idField
     * @memberof CouchDbNano
     * @returns {Object} Modified entity
     */
    beforeSaveTransformID(entity, idField) {
        let newEntity = _.cloneDeep(entity);
        if (idField !== "_id" && newEntity[idField] !== undefined) {
            newEntity._id = newEntity[idField];
            delete newEntity[idField];
        }
        return newEntity;
    }

    /**
     * Transforms MongoDB's '_id' into user defined 'idField'
     * @param {Object} entity
     * @param {String} idField
     * @memberof CouchDbNano
     * @returns {Object} Modified entity
     */
    afterRetrieveTransformID(entity, idField) {
        if (idField !== "_id") {
            entity[idField] = entity["_id"];
            delete entity._id;
        }
        return entity;
    }
}

module.exports = CouchDbNanoAdapter;
