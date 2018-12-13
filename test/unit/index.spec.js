"use strict";

const {ServiceBroker} = require("moleculer");

const CouchDbNanoAdapter = require("../../src");
const Nano = require("nano");

function protectReject(err) {
    if (err && err.stack) {
        console.error(err);
        console.error(err.stack);
    }
    expect(err).toBeDefined();
}

describe("Test CouchDbNanoAdapter", () => {
    const broker = new ServiceBroker({logger: false});
    const service = broker.createService({
        name: "store",
        collection: "posts"
    });
    const uri = "";
    const opts = {};
    const adapter = new CouchDbNanoAdapter(uri, opts);

    it("should be created", () => {
        expect(adapter).toBeDefined();
        expect(adapter.uri).toBe(uri);
        expect(adapter.opts).toBe(opts);
        expect(adapter.init).toBeInstanceOf(Function);
        expect(adapter.connect).toBeInstanceOf(Function);
        expect(adapter.disconnect).toBeInstanceOf(Function);
        expect(adapter.find).toBeInstanceOf(Function);
        expect(adapter.findOne).toBeInstanceOf(Function);
        expect(adapter.findById).toBeInstanceOf(Function);
        expect(adapter.findByIds).toBeInstanceOf(Function);
        expect(adapter.count).toBeInstanceOf(Function);
        expect(adapter.insert).toBeInstanceOf(Function);
        expect(adapter.insertMany).toBeInstanceOf(Function);
        expect(adapter.updateMany).toBeInstanceOf(Function);
        expect(adapter.updateById).toBeInstanceOf(Function);
        expect(adapter.removeMany).toBeInstanceOf(Function);
        expect(adapter.removeById).toBeInstanceOf(Function);
        expect(adapter.clear).toBeInstanceOf(Function);
        expect(adapter.beforeSaveTransformID).toBeInstanceOf(Function);
        expect(adapter.afterRetrieveTransformID).toBeInstanceOf(Function);
    });

    it("throw error in init if 'collection' is not defined", () => {
        expect(() => {
            service.schema.collection = undefined;
            adapter.init(broker, service);
        }).toThrow("Missing `collection` definition in schema of service!");
    });

    it("call init", () => {
        service.schema.collection = "posts";
        adapter.init(broker, service);
        expect(adapter.broker).toBe(broker);
        expect(adapter.service).toBe(service);
    });

    it("call connect with default params", () => {
        adapter.init(broker, service);
        return adapter.connect()
            .then(() => {
                expect(adapter.db).toBeInstanceOf(Object);
            })
            .catch(protectReject);
    });

    it("call connect with uri", () => {
        adapter.init(broker, service);
        adapter.uri = 'couchdb://localhost:5984';
        return adapter.connect(uri)
            .then(() => {
                expect(adapter.db).toBeInstanceOf(Object);
            })
            .catch(protectReject);
    });

    it("call connect with uri & opts", () => {
        adapter.init(broker, service);
        adapter.uri = 'couchdb://localhost:5984';
        adapter.opts = {};
        return adapter.connect(uri)
            .then(() => {
                expect(adapter.db).toBeInstanceOf(Object);
            })
            .catch(protectReject);
    });

    // describe("Test createCursor", () => {
    //     adapter.collection = fakeCollection;
    //
    //     it("init", () => {
    //         adapter.collection.find = jest.fn(() => ({
    //             sort: jest.fn(),
    //             project: jest.fn(),
    //             skip: jest.fn(),
    //             limit: jest.fn(),
    //             toArray: toArrayCB
    //         }));
    //     });
    //
    //     it("call without params", () => {
    //         adapter.collection.find.mockClear();
    //         adapter.createCursor();
    //         expect(adapter.collection.find).toHaveBeenCalledTimes(1);
    //         expect(adapter.collection.find).toHaveBeenCalledWith({});
    //     });
    //
    //     it("call without params & count", () => {
    //         adapter.collection.countDocuments.mockClear();
    //         adapter.collection.find.mockClear();
    //
    //         adapter.createCursor(null, true);
    //
    //         expect(adapter.collection.find).toHaveBeenCalledTimes(0);
    //         expect(adapter.collection.countDocuments).toHaveBeenCalledTimes(1);
    //         expect(adapter.collection.countDocuments).toHaveBeenCalledWith({});
    //     });
    //
    //     it("call with query", () => {
    //         adapter.collection.find.mockClear();
    //         let query = {};
    //         adapter.createCursor({query});
    //         expect(adapter.collection.find).toHaveBeenCalledTimes(1);
    //         expect(adapter.collection.find).toHaveBeenCalledWith(query);
    //     });
    //
    //     it("call with query & count", () => {
    //         adapter.collection.countDocuments.mockClear();
    //         let query = {};
    //         adapter.createCursor({query}, true);
    //         expect(adapter.collection.countDocuments).toHaveBeenCalledTimes(1);
    //         expect(adapter.collection.countDocuments).toHaveBeenCalledWith(query);
    //     });
    //
    //     it("call with sort string", () => {
    //         adapter.collection.find.mockClear();
    //         let query = {};
    //         let q = adapter.createCursor({query, sort: "-votes title"});
    //         expect(adapter.collection.find).toHaveBeenCalledTimes(1);
    //         expect(adapter.collection.find).toHaveBeenCalledWith(query);
    //
    //         expect(q.sort).toHaveBeenCalledTimes(1);
    //         expect(q.sort).toHaveBeenCalledWith({votes: -1, title: 1});
    //     });
    //
    //     it("call with sort array", () => {
    //         adapter.collection.find.mockClear();
    //         let query = {};
    //         let q = adapter.createCursor({query, sort: ["createdAt", "title"]});
    //         expect(adapter.collection.find).toHaveBeenCalledTimes(1);
    //         expect(adapter.collection.find).toHaveBeenCalledWith(query);
    //
    //         expect(q.sort).toHaveBeenCalledTimes(1);
    //         expect(q.sort).toHaveBeenCalledWith({createdAt: 1, title: 1});
    //     });
    //
    //     it("call with sort object", () => {
    //         adapter.collection.find.mockClear();
    //         let query = {};
    //         let q = adapter.createCursor({query, sort: {createdAt: 1, title: -1}});
    //         expect(adapter.collection.find).toHaveBeenCalledTimes(1);
    //         expect(adapter.collection.find).toHaveBeenCalledWith(query);
    //
    //         expect(q.sort).toHaveBeenCalledTimes(1);
    //         expect(q.sort).toHaveBeenCalledWith({createdAt: 1, title: -1});
    //     });
    //
    //     it("call with limit & offset", () => {
    //         adapter.collection.find.mockClear();
    //         let q = adapter.createCursor({limit: 5, offset: 10});
    //         expect(adapter.collection.find).toHaveBeenCalledTimes(1);
    //         expect(adapter.collection.find).toHaveBeenCalledWith(undefined);
    //
    //         expect(q.limit).toHaveBeenCalledTimes(1);
    //         expect(q.limit).toHaveBeenCalledWith(5);
    //         expect(q.skip).toHaveBeenCalledTimes(1);
    //         expect(q.skip).toHaveBeenCalledWith(10);
    //     });
    //
    //     it("call with full-text search", () => {
    //         adapter.collection.find.mockClear();
    //         let q = adapter.createCursor({search: "walter"});
    //         expect(adapter.collection.find).toHaveBeenCalledTimes(2);
    //         expect(adapter.collection.find).toHaveBeenCalledWith({
    //             "$text": {"$search": "walter"}
    //         });
    //         expect(q.project).toHaveBeenCalledTimes(1);
    //         expect(q.project).toHaveBeenCalledWith({
    //             "_score": {"$meta": "textScore"}
    //         });
    //
    //         expect(q.sort).toHaveBeenCalledTimes(1);
    //         expect(q.sort).toHaveBeenCalledWith({"_score": {"$meta": "textScore"}});
    //     });
    //
    // });

    it("call insert", () => {
        return adapter.insert({_id: '1', a: 1, b: 2})
            .then(res => expect(res._id).toBe('1'))
            .catch(protectReject);
    });

    it("call findById", () => {
        return adapter.findById('1')
            .then(res => expect(res._id).toBe('1'))
            .catch(protectReject);
    });

    it("call find", () => {
        return adapter.find({query: {a: 1}})
            .then(res => {
                expect(res).toBeInstanceOf(Array);
                expect(res[0].a).toBe(1);
            })
            .catch(protectReject);
    });

    it("call findOne", () => {
        return adapter.find({query: {a: 1}})
            .then(res => {
                expect(res).toBeInstanceOf(Array);
                expect(res).toHaveLength(1);
                expect(res[0]).toMatchObject({a: 1});
            })
            .catch(protectReject);
    });

    it("call removeById", () => {
        return adapter.removeById('1')
            .then(res => expect(res._id).toBe('1'))
            .catch(protectReject);
    });

    it("call insertMany", () => {
        let entities = [
            {_id: '2', a: 2, b: 20},
            {_id: '3', a: 3, b: 20},
            {_id: '4', a: 4, b: 20}
        ];
        return adapter.insertMany(entities)
            .then(res => {
                expect(res).toBeInstanceOf(Array);
                expect(res).toHaveLength(3);
            })
            .catch(protectReject);
    });

    it("call updateMany", () => {
        let query = {b: 20};
        let update = {c: 100};
        return adapter.updateMany(query, update)
            .then(res => {
                expect(res).toBeGreaterThanOrEqual(3);
            })
            .catch(protectReject);
    });

    it("call count", () => {
        let query = {b: 20};
        return adapter.count({query})
            .then(res => {
                expect(res).toBeGreaterThanOrEqual(3);
            })
            .catch(protectReject);
    });

    it("call findByIds", () => {
        return adapter.findByIds(['2', '3', '4'])
            .then(res => {
                expect(res).toBeInstanceOf(Array);
                expect(res).toHaveLength(3);
            })
            .catch(protectReject);
    });

    it("call removeMany", () => {
        let query = {b: 20};
        return adapter.removeMany(query)
            .then(res => {
                expect(res).toBeGreaterThanOrEqual(3);
            })
            .catch(protectReject);
    });







    //
    // it("call count", () => {
    //     adapter.createCursor = jest.fn(() => Promise.resolve(8));
    //
    //     let params = {};
    //     return adapter.count(params).catch(protectReject).then(() => {
    //         expect(adapter.createCursor).toHaveBeenCalledTimes(1);
    //         expect(adapter.createCursor).toHaveBeenCalledWith(params, true);
    //     });
    // });
    //
    //
    //
    // it("call updateById", () => {
    //     doc.toJSON.mockClear();
    //
    //     let update = {};
    //     return adapter.updateById(5, update).catch(protectReject).then(res => {
    //         expect(res).toEqual(doc);
    //         expect(adapter.collection.findOneAndUpdate).toHaveBeenCalledTimes(1);
    //         expect(adapter.collection.findOneAndUpdate).toHaveBeenCalledWith({_id: 5}, update, {returnOriginal: false});
    //     });
    // });
    //


    //
    // it("call clear", () => {
    //     adapter.collection.deleteMany.mockClear();
    //     return adapter.clear().catch(protectReject).then(() => {
    //         expect(adapter.collection.deleteMany).toHaveBeenCalledTimes(1);
    //         expect(adapter.collection.deleteMany).toHaveBeenCalledWith({});
    //     });
    // });
    //
    // it("call entityToObject", () => {
    //     adapter.objectIdToString = jest.fn();
    //     doc._id = null;
    //     adapter.entityToObject(doc);
    //     expect(adapter.objectIdToString).toHaveBeenCalledTimes(0);
    //
    //     doc._id = 1;
    //     adapter.entityToObject(doc);
    //     expect(adapter.objectIdToString).toHaveBeenCalledTimes(1);
    // });
    //
    // it("should transform idField into _id", () => {
    //     adapter.stringToObjectId = jest.fn(entry => entry);
    //
    //     let entry = {
    //         myID: "123456789",
    //         title: "My first post"
    //     };
    //     let idField = "myID";
    //
    //     let res = adapter.beforeSaveTransformID(entry, idField);
    //
    //     expect(res.myID).toEqual(undefined);
    //     expect(res._id).toEqual(entry.myID);
    // });
    //
    // it("should NOT transform idField into _id", () => {
    //     // MongoDB will generate the _id
    //     let entry = {
    //         title: "My first post"
    //     };
    //     let idField = "myID";
    //
    //     let res = adapter.beforeSaveTransformID(entry, idField);
    //
    //     expect(res.myID).toEqual(undefined);
    //     expect(res._id).toEqual(undefined);
    // });
    //
    // it("should transform _id into idField", () => {
    //     adapter.objectIdToString = jest.fn(entry => entry);
    //
    //     let entry = {
    //         _id: "123456789",
    //         title: "My first post"
    //     };
    //     let idField = "myID";
    //
    //     let res = adapter.afterRetrieveTransformID(entry, idField);
    //
    //     expect(res.myID).toEqual(entry.myID);
    //     expect(res._id).toEqual(undefined);
    // });
    //
    // it("should NOT transform _id into idField", () => {
    //     let entry = {
    //         _id: "123456789",
    //         title: "My first post"
    //     };
    //     let idField = "_id";
    //
    //     let res = adapter.afterRetrieveTransformID(entry, idField);
    //
    //     expect(res.myID).toEqual(undefined);
    //     expect(res._id).toEqual(entry._id);
    // });
    it("call disconnect", () => {
        return adapter.disconnect()
            .then(() => {
                expect(adapter.db).toBe(null);
            })
            .catch(protectReject);
    });
});

