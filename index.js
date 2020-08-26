var DB = require('k-sync').DB;
var mongoAql = require('mongo-aql');
var arangojs = require('arangojs');
var uuid = require('uuid');

module.exports = SyncArango;

// url: string | Array<string>
function SyncArango(urls, options) {
	// use without new
	if (!(this instanceof SyncArango)) {
		return new SyncArango(urls, options);
	}

	if (!options) options = {};

	// By default, we create indexes on any ops collection that is used
	this.disableIndexCreation = options.disableIndexCreation || false;

	// The getOps() method depends on a collectionname_ops collection, and that
	// collection should have an index on the operations stored there. We could
	// ask people to make these indexes themselves, but by default the mongo
	// driver will do it automatically. This approach will leak memory relative
	// to the number of collections you have. This should be OK, as we are not
	// expecting thousands of mongo collections.

	// Map from collection name -> true for op collections we've ensureIndex'ed
	this.opIndexes = {};

	// Track whether the close method has been called
	this.closed = false;

	if (options.arango) {
		this.arango = options.arango;
	}
	else if (typeof urls === 'string' || Array.isArray(urls)) {
		// We can only get the mongodb client instance in a callback, so
		// buffer up any requests received in the meantime
		this.arango = null;
		this._connect(urls, options);
	}
	else {
		throw Error('Arango url missing/invalid (' + urls + ')');
	}
};

SyncArango.prototype = Object.create(DB.prototype);

SyncArango.prototype.projectsSnapshots = true;

SyncArango.prototype._connect = function(urls, options) {
	var dbName, username, password, url;

	if (Array.isArray(urls)) {
		urls = urls.map(parseUrl);
	}
	else {
		urls = parseUrl(urls);
	}

	// get username, password, dbName
	function parseUrl(url) {
		const urlParsed = require('url').parse(url);
		const auth = urlParsed.auth.split(':');

		if (auth.length) {
			username = auth[0];
			password = auth[1];
		}

		if (urlParsed.path && urlParsed.path !== '/') {
			dbName = urlParsed.path.substring(1);
			url = urlParsed.protocol + '//' + urlParsed.host;
		}

		return url;
	}

	if (!dbName) {
		throw new Error('Database not found: ', dbName);
	}

	const config = { url: urls };

	if (options.ca) {
		config.agentOptions = { ca: require('fs').readFileSync(options.ca) };
	}

	this.arango = new arangojs.Database(config);

	if (username && username) {
		this.arango.useBasicAuth(username, password);
	}

	this.arango.useDatabase(dbName);
};

/*
** We'll be creating collections on the fly, because that's handy.
*/
SyncArango.prototype._createCollection = async function(collectionName){
	var db = this.arango;

	try {
		const collection = db.collection(collectionName);
		await collection.create();

		const oplogCollection = db.collection(this.getOplogCollectionName(collectionName));
		await oplogCollection.create();
	}
	catch (err) {
		throw error(err, collectionName);
	}
};

SyncArango.prototype._getCollection = function(collectionName) {
	var db = this.arango;

	var err = this.validateCollectionName(collectionName);
	if (err) throw err;

	return db.collection(collectionName);
};


// Get and return the op collection from mongo, ensuring it has the op index.
SyncArango.prototype._getOpCollection = async function(collectionName) {
	const db = this.arango;
	const name = this.getOplogCollectionName(collectionName);
	const collection = db.collection(name);

	// Given the potential problems with creating indexes on the fly, it might
	// be preferrable to disable automatic creation
	if (this.disableIndexCreation) {
		return collection;
	}

	if (this.opIndexes[collectionName]) {
		return collection;
	}

	// return for now
	return collection;

	// WARNING: Creating indexes automatically like this is quite dangerous in
	// production if we are starting with a lot of data and no indexes
	// already. If new indexes were added or definition of these indexes were
	// changed, users upgrading this module could unsuspectingly lock up their
	// databases. If indexes are created as the first ops are added to a
	// collection this won't be a problem, but this is a dangerous mechanism.
	// Perhaps we should only warn instead of creating the indexes, especially
	// when there is a lot of data in the collection.

	try {
		const index = await collection.createIndex({ type: "hash", fields: [ 'd', 'v' ] });
		this.opIndexes[collectionName] = true;
	}
	catch (error) {
		console.warn('Warning: Could not create index for op collection:', error.toString());
		console.log({ collectionName, name });
		console.log({ collection });
	}
	
	return collection;
};


// **** Commit methods

SyncArango.prototype.commit = async function(collectionName, id, op, snapshot, options, callback) {
	try {
		const result = await this._writeOp(collectionName, id, op, snapshot);

		var opId = result._key;
		const succeeded = await this._writeSnapshot(collectionName, id, snapshot, opId);

		if (succeeded) return callback(null, succeeded);

		// Cleanup unsuccessful op if snapshot write failed. This is not
		// neccessary for data correctness, but it gets rid of clutter
		await this._deleteOp(collectionName, opId);
		callback();
	}
	catch (err) {
		callback(err);
	}
};

SyncArango.prototype._writeOp = async function(collectionName, id, op, snapshot, retry) {
	if (typeof op.v !== 'number') {
		const err = {
			code: 4101,
			message: 'Invalid op version ' + collectionName + '.' + id + ' ' + op.v
		};

		throw err;
	}

	try {
		const opCollection = await this._getOpCollection(collectionName);
		const doc = shallowClone(op);

		doc.d = id;
		doc.o = snapshot._opLink;

		const result = await opCollection.save(doc);

		return result;
	}
	catch (err) {
		// ArangoError: AQL: conflict (while executing)
		// Try 100 times if this error happens
		// https://github.com/arangodb/arangodb/issues/2903 
		// rocksdb racing condition (to write)
		console.log(err.toString());
		console.log({retry});

		if (err.errorNum == 1200 && (!retry || retry < 100)) {
			if (retry) {
				retry++;
			}
			else {
				retry = 1;
			}
			
			console.log('retry _writeOp', collectionName, id, retry);
			this._writeOp(collectionName, id, snapshot, opLink, retry)
		}
		else  {
			throw error(err, collectionName, id, snapshot);
			// throw err.toString();
			// console.trace();
			// console.log(err.toString());
			// console.log(err);
		}		
	}
};

SyncArango.prototype._deleteOp = async function(collectionName, opId, callback) {
	const opCollection = await this._getOpCollection(collectionName);
	const result = await opCollection.remove(opId);

	return result;
};

SyncArango.prototype._writeSnapshot = async function(collectionName, id, snapshot, opLink, retry) {
	var doc;
	// console.log('_writeSnapshot', collectionName, id);

	try {
		const collection = this._getCollection(collectionName);

		if (!collection) {
			console.error('Collection not found (_writeSnapshot)', collectionName);
			return false;
		}


		doc = castToDoc(id, snapshot, opLink);

		if (doc._v === 1) {
			await collection.save(doc)

			if (retry) {
				console.log('retry succeeded', { collectionName, id, retry });
			}

			return true;
		} else {
			const result = await collection.replaceByExample({_key: id, _v: doc._v - 1}, doc);
			const succeeded = result && !!result.replaced;

			if (retry) {
				console.log('retry succeeded', { collectionName, id, retry });
			}

			return succeeded;
		}
	}
	catch (err) {
		// ArangoError: AQL: conflict (while executing)
		// Try 100 times if this error happens
		// https://github.com/arangodb/arangodb/issues/2903 
		// rocksdb racing condition (to write)
		console.log('');
		console.log(err.toString());
		console.log({retry});

		if (err.errorNum == 1200 && (!retry || retry < 100)) {
			if (retry) {
				retry++;
			}
			else {
				retry = 1;
			}

			console.log('retry _writeSnapshot', collectionName, id, retry);
			this._writeSnapshot(collectionName, id, snapshot, opLink, retry)
		}
		else  {
			error(err, collectionName, id, snapshot, doc);
			// throw err.toString();
			// console.trace();
			// console.log(err.toString());
			// console.log(err);
		}
	}
};


// **** Snapshot methods

SyncArango.prototype.getSnapshot = async function(collectionName, id, fields, options, callback) {
	try {
		const collection = this._getCollection(collectionName);
		const projection = getProjection(fields, options);

		const doc = await collection.document(id);
		const snapshot = doc ? castToProjectedSnapshot(doc, projection) : new ArangoSnapshot(id, 0, null, null);

		callback(null, snapshot);
	}
	catch (err) {
		// 1202 is "document not found"
		// 1203 is "collection not found"
		// in that case we'll create the collection and return empty array
		if (err.errorNum === 1203) {
			// create the missing collection and try again
			await this._createCollection(collectionName);
			return await this.getSnapshot(collectionName, id, fields, options, callback);
		}
		// we just return 'undefined'
		else if (err.errorNum === 1202) {
			const snapshot = new ArangoSnapshot(id, 0, null, null);
			return callback(null, snapshot);			
		}

		callback(error(err, collectionName, id));
	}
};

SyncArango.prototype.getSnapshotBulk = async function(collectionName, ids, fields, options, callback) {
	const db = this.arango;

	try {
		const queryObject = { _key: { $in: ids } };
		const q = mongoAql(collectionName, queryObject);
		const projection = getProjection(fields, options);
		const cursor = await db.query(q.query, q.values);
		const data = await cursor.all();
		const snapshotMap = {};
		const uncreated = [];

		sortResultsByIds(data, ids);

		for (var i = 0; i < data.length; i++) {
			var snapshot = castToProjectedSnapshot(data[i], projection);
			snapshotMap[snapshot.id] = snapshot;
		}

		for (var i = 0; i < ids.length; i++) {
			var id = ids[i];
			if (snapshotMap[id]) continue;
			snapshotMap[id] = new ArangoSnapshot(id, 0, null, null);
		}

		callback(null, snapshotMap);
	}
	catch (err) {
		if (err.errorNum === 1203) {
			await this._createCollection(collectionName);
			return await this.getSnapshotBulk(collectionName, ids, fields, options, callback);
		}

		return callback(error(err), []);
	}
};


// **** Oplog methods

// Overwrite me if you want to change this behaviour.
SyncArango.prototype.getOplogCollectionName = function(collectionName) {
	return 'ops_' + collectionName;
};

SyncArango.prototype.validateCollectionName = function(collectionName) {
	if (
		typeof collectionName !== 'string' ||
		collectionName === 'system' || (
			collectionName[0] === 'o' &&
			collectionName[1] === 'p' &&
			collectionName[2] === 's' &&
			collectionName[3] === '_'
		)
	) {
		console.trace();
		return {code: 4102, message: 'Invalid collection name ' + collectionName};
	}
};

SyncArango.prototype.getOpsToSnapshot = async function(collectionName, id, from, snapshot, options, callback) {
	if (snapshot._opLink == null) {
		var err = getSnapshotOpLinkError(collectionName, id);
		return callback(err);
	}

	try {
		const ops = await this._getOps(collectionName, id, from, options);
		var filtered = getLinkedOps(ops, null, snapshot._opLink);
		var err = checkOpsFrom(collectionName, id, filtered, from);

		if (err) return callback(err);

		callback(null, filtered);
	}
	catch (err) {
		callback(err);
	}
};

SyncArango.prototype.getOps = async function(collectionName, id, from, to, options, callback) {
	try {
		const doc = await this._getSnapshotOpLink(collectionName, id);

		if (doc) {
			if (isCurrentVersion(doc, from)) {
				return callback(null, []);
			}

			var err = doc && checkDocHasOp(collectionName, id, doc);
			if (err) return callback(err);
		}


		const ops = await this._getOps(collectionName, id, from, options);
		var filtered = filterOps(ops, doc, to);
		var err = checkOpsFrom(collectionName, id, filtered, from);

		if (err) return callback(err);

		callback(null, filtered);
	}
	catch (err) {
		callback(err);
	}
};

SyncArango.prototype.getOpsBulk = async function(collectionName, fromMap, toMap, options, callback) {
	var ids = Object.keys(fromMap);

	try {
		const docs = await this._getSnapshotOpLinkBulk(collectionName, ids);
		const docMap = getDocMap(docs);

		// Add empty array for snapshot versions that are up to date and create
		// the query conditions for ops that we need to get
		const conditions = [];
		const opsMap = {};

		for (var i = 0; i < ids.length; i++) {
			var id = ids[i],
				doc = docMap[id],
				from = fromMap[id];

			if (doc) {
				if (isCurrentVersion(doc, from)) {
					opsMap[id] = [];
					continue;
				}

				var err = checkDocHasOp(collectionName, id, doc);
				if (err) return callback(err);
			}

			conditions.push({
				d: id,
				v: { $gte: from }
			});
		}

		// Return right away if none of the snapshot versions are newer than the
		// requested versions
		if (!conditions.length) return callback(null, opsMap);

		// Otherwise, get all of the ops that are newer
		const opsBulk = await this._getOpsBulk(collectionName, conditions, options);

		for (var i = 0; i < conditions.length; i++) {
			var id = conditions[i].d;
			var ops = opsBulk[id];
			var doc = docMap[id];
			var from = fromMap[id];
			var to = toMap && toMap[id];
			var filtered = filterOps(ops, doc, to);
			var err = checkOpsFrom(collectionName, id, filtered, from);
			if (err) {
				console.error(err);
				return callback(err);
			}
			opsMap[id] = filtered;
		}


		callback(null, opsMap);
	}
	catch (err) {
		callback(err);
	}
};

function checkOpsFrom(collectionName, id, ops, from) {
	if (ops.length === 0) return;
	if (ops[0] && ops[0].v === from) return;
	if (from == null) return;

	return {
		code: 5103,
		message: 'Missing ops from requested version ' + collectionName + '.' + id + ' ' + from
	}
};

function getSnapshotOpLinkError(collectionName, id) {
	return {
		code: 5102,
		message: 'Snapshot missing last operation field "_o" ' + collectionName + '.' + id
	};
}

function checkDocHasOp(collectionName, id, doc) {
	if (doc._o) return;
	return getSnapshotOpLinkError(collectionName, id);
}

function isCurrentVersion(doc, version) {
	return doc._v === version;
}

function getDocMap(docs) {
	var docMap = {};

	for (var i = 0; i < docs.length; i++) {
		var doc = docs[i];
		docMap[doc._key] = doc;
	}

	return docMap;
}

function filterOps(ops, doc, to) {
	// Always return in the case of no ops found whether or not consistent with
	// the snapshot
	if (!ops) return [];
	if (!ops.length) return ops;
	if (!doc) {
		// There is no snapshot currently. We already returned if there are no
		// ops, so this could happen if:
		//   1. The doc was deleted
		//   2. The doc create op is written but not the doc snapshot
		//   3. Same as 3 for a recreate
		//   4. We are in an inconsistent state because of an error
		//
		// We treat the snapshot as the canonical version, so if the snapshot
		// doesn't exist, the doc should be considered deleted. Thus, a delete op
		// should be in the last version if no commits are inflight or second to
		// last version if commit(s) are inflight. Rather than trying to detect
		// ops inconsistent with a deleted state, we are simply returning ops from
		// the last delete. Inconsistent states will ultimately cause write
		// failures on attempt to commit.
		//
		// Different delete ops must be identical and must link back to the same
		// prior version in order to be inserted, so if there are multiple delete
		// ops at the same version, we can grab any of them for this method.
		// However, the _id of the delete op might not ultimately match the delete
		// op that gets maintained if two are written as a result of two
		// simultanous delete commits. Thus, the _id of the op should *not* be
		// assumed to be consistent in the future.
		var deleteOp = getLatestDeleteOp(ops);
		// Don't return any ops if we don't find a delete operation, which is the
		// correct thing to do if the doc was just created and the op has been
		// written but not the snapshot. Note that this will simply return no ops
		// if there are ops but the snapshot doesn't exist.
		if (!deleteOp) return [];
		return getLinkedOps(ops, to, deleteOp.d);
	}

	return getLinkedOps(ops, to, doc._o);
}

function getLatestDeleteOp(ops) {
	for (var i = ops.length; i--;) {
		var op = ops[i];
		if (op.del) return op;
	}
}

function getLinkedOps(ops, to, link) {
	var linkedOps = []
	for (var i = ops.length; i-- && link;) {
		var op = ops[i];
		if (link.equals ? !link.equals(op._key) : link !== op._key) continue;
		link = op.o;
		if (to == null || op.v < to) {
			delete op._id;
			delete op._key;
			delete op.o;
			linkedOps.unshift(op);
		}
	}
	return linkedOps;
}

SyncArango.prototype._getOps = async function(collectionName, id, from, options) {
	var db = this.arango;

	var queryObject = {
		d: id,
		v: {$gte: from},
		$orderby: {v: 1}
	};

	// Exclude the `d` field, which is only for use internal to livedb-mongo.
	// Also exclude the `m` field, which can be used to store metadata on ops
	// for tracking purposes
	try {
		const projection = (options && options.metadata) ? {d: 0} : {d: 0, m: 0};
		const q = mongoAql(this.getOplogCollectionName(collectionName), queryObject);
		const cursor = await db.query(q.query, q.values);
		const data = await cursor.all();

		// Strip out d, m in the results
		for (var i = 0; i < data.length; i++) {
			delete data[i].d;
			delete data[i].m;
		}

		return data;
	}
	catch (err) {
		// 1202 is "document not found"
		// 1203 is "collection not found"
		// in that case we'll create the collection and return empty array
		if (err.errorNum === 1203) {
			await this._createCollection(collectionName);
			return await this._getOps(collectionName, id, from, options);
		}
		// if nothing was found, we don't return an error, just empty set
		else if (err.errorNum === 1202) {
			return [];
		}

		throw error(err);
	}
};

SyncArango.prototype._getOpsBulk = async function(collectionName, conditions, options) {
	var db = this.arango;

	try {
		const opsCollectionName = this.getOplogCollectionName(collectionName);

		var queryObject = {
				$or: conditions,
				$orderby: {d: 1, v: 1}
			},
			q = mongoAql(opsCollectionName, queryObject);

		// Exclude the `m` field, which can be used to store metadata on ops for
		// tracking purposes
		// do this in readOpsBulk
		var projection = (options && options.metadata) ? null : {m: 0};
		const cursor = await db.query(q.query, q.values);
		const opsMap = await readOpsBulk(cursor);

		return opsMap;
	}
	catch (err) {
		// 1202 is "document not found"
		// in that case we'll create the collection and return empty array
		if (err.errorNum === 1203) {
			await this._createCollection(collectionName);
			return await this._getOpsBulk(collectionName, conditions, options);
		}

		return {};
	};
};

async function readOpsBulk(cursor) {
	var opsMap = {};
	var op;

	try {
		const results = await cursor.all();

		results.forEach((op) => {

			if (op) {
				opsMap[op.d] = opsMap[op.d] || [];
				opsMap[op.d].push(op);

				delete op.d;
				delete op.m;
			}
		});

		return opsMap;
	}
	catch (err) {
		throw error(err);
	}
}

SyncArango.prototype._getSnapshotOpLink = async function(collectionName, id) {
	var db = this.arango;

	const collection = this._getCollection(collectionName);
	const projection = {_id: 0, _o: 1, _v: 1};

	try {
		const doc = await collection.document(id);

		return castToProjected(doc, projection);
	}
	catch (err) {
		// not found, we return null
		if (err.errorNum === 1202) {
			return null;
		}
		// collection not found, we create a collection and try again
		else if (err.errorNum === 1203) {
			await this._createCollection(collectionName);
			const result = await this._getSnapshotOpLink(collectionName, id);

			return result;
		}

		throw error(err);
	}
};

SyncArango.prototype._getSnapshotOpLinkBulk = async function(collectionName, ids) {
	const db = this.arango;
	const queryObject = { _key: { $in: ids } };
	const q = mongoAql(collectionName, queryObject);
	const projection = { _key: 1, _id: 1, _o: 1, _v: 1 };

	try {
		const cursor = await db.query(q.query, q.values);
		const data = await cursor.all();
		const res = [];

		for (var i = 0; i < data.length; i++) {
			res.push(castToProjected(data[i], projection));
		}

		return res;
	}
	catch (err) {
		// create collection and try again
		if (err.errorNum === 1203) {
			await this._createCollection(collectionName);
			return await this._getSnapshotOpLinkBulk(collectionName, ids);
		}

		throw error(err);
	}
};


SyncArango.prototype.query = async function(collectionName, inputQuery, fields, options, callback) {
	var db = this.arango, q,
		normalizedInputQuery = normalizeQuery(inputQuery);

	if (!collectionName) {
		return callback('collection name empty')
	}

	try {
		const projection = getProjection(fields, options);
		q = mongoAql(collectionName, normalizedInputQuery);
		const cursor = await db.query(q.query, q.values);
		const data = await cursor.map(castToProjectedSnapshotFunction(projection));

		// we want to maintain the order if we are getting an array of items (for example a pathquery)
		if (Array.isArray(inputQuery)) {
			sortResultsByIds(data, inputQuery);
		}

		callback(null, data);
	}
	catch (err) {
		if (err.errorNum === 1203) {
			await this._createCollection(collectionName);
			return this.query(collectionName, inputQuery, fields, options, callback);
		}

		return callback(error(err, q));
	}
};

SyncArango.prototype.queryPoll = async function(collectionName, inputQuery, options, callback) {
	var db = this.arango, q,
		normalizedInputQuery = normalizeQuery(inputQuery);

	try {
		var projection = { _key: 1 },
			q = mongoAql(collectionName, normalizedInputQuery);

		// self._query(collection, normalizedInputQuery, projection, function(err, results, extra) {
		const cursor = await db.query(q.query, q.values);
		const data = await cursor.all();

		var ids = [];

		for (var i = 0; i < data.length; i++) {
			ids.push(data[i]._key);
		}

		// we want to maintain the order if we are getting an array of items
		if (Array.isArray(inputQuery)) {
			sortResultsByIds(ids, inputQuery);
		}

		callback(null, ids);
	}
	catch (err) {
		if (err.errorNum === 1203) {
			await this._createCollection(collectionName);
			return this.queryPoll(collectionName, inputQuery, options, callback);
		}

		return callback(err);
	}

};

function sortResultsByIds(results, ids) {
	var fn = function(a, b) { return ids.indexOf(a.id? a.id: a) - ids.indexOf(b.id? b.id: b) };
	results.sort(fn);
}

SyncArango.prototype.queryPollDoc = async function(collectionName, id, query, options, callback) {
	var db = this.arango;

	query = normalizeQuery(query);

	// Run the query against a particular mongo document by adding an _id filter
	var queryId = query._key;

	if (queryId && typeof queryId === 'object') {
		// Check if the query contains the id directly in the common pattern of
		// a query for a specific list of ids, such as {_id: {$in: [1, 2, 3]}}
		if (Array.isArray(queryId.$in) && Object.keys(queryId).length === 1) {
			if (queryId.$in.indexOf(id) === -1) {
				// If the id isn't in the list of ids, then there is no way this
				// can be a match
				return callback();
			} else {
				// If the id is in the list, then it is equivalent to restrict to our
				// particular id and override the current value
				query._key = id;
			}
		} else {
			delete query._id;
			delete query._key;

			query.$and = (query.$and) ?
				query.$and.concat({_key: id}, {_key: queryId}) :
				[{_key: id}, {_key: queryId}];
		}
	} else if (queryId && queryId !== id) {
		// If queryId is a primative value such as a string or number and it
		// isn't equal to the id, then there is no way this can be a match
		return callback();
	} else {
		// Restrict the query to this particular document
		query._key = id;
	}

	try {
		var q = mongoAql(collectionName, query);
	}
	catch (err) {
		return callback(err);
	}

	try {
		const cursor = await db.query(q.query, q.values);
		const data = await cursor.all();

		callback(null, data && data.length > 0);
	}
	catch (err) {
		if (err && err.errorNum === 1203) {
			await this._createCollection(collectionName);
			return this.queryPollDoc(collectionName, id, query, options, callback);
		}

		callback(error(err));
	}
};

// **** Polling optimization

// Can we poll by checking the query limited to the particular doc only?
SyncArango.prototype.canPollDoc = function(collectionName, query) {
	return !(
		query.hasOwnProperty('$orderby') ||
		query.hasOwnProperty('$limit') ||
		query.hasOwnProperty('$skip') ||
		query.hasOwnProperty('$count')
	);
};

// Return true to avoid polling if there is no possibility that an op could
// affect a query's results
SyncArango.prototype.skipPoll = function(collectionName, id, op, query) {
	// Livedb is in charge of doing the validation of ops, so at this point we
	// should be able to assume that the op is structured validly
	if (op.create || op.del) return false;
	if (!op.op) return true;
	var fields = getFields(query);
	return !opContainsAnyField(op.op, fields);
};

function getFields(query) {
	var fields = {};
	getInnerFields(query.$query, fields);
	getInnerFields(query.$orderby, fields);
	getInnerFields(query, fields);
	return fields;
}

function getInnerFields(params, fields) {
	if (!params) return;
	for (var key in params) {
		var value = params[key];
		if (key === '$or' || key === '$and') {
			for (var i = 0; i < value.length; i++) {
				var item = value[i];
				getInnerFields(item, fields);
			}
		} else if (key[0] !== '$') {
			var property = key.split('.')[0];
			fields[property] = true;
		}
	}
}

function opContainsAnyField(op, fields) {
	for (var i = 0; i < op.length; i++) {
		var component = op[i];
		if (component.p.length === 0) {
			return true;
		} else if (fields[component.p[0]]) {
			return true;
		}
	}
	return false;
}



// graph operations

// getNeighbors
// gets neighbors of a vertex example
// edgeData can be supplied to filter the neighbors, can be null/undefined to get all the neighbor of a particular vertex
// Options can hold a direction (outbound/inbound/any)
// Returns a list of vertex keys
// 
SyncArango.prototype.getNeighbors = async function(graphName, vertex, edgeData, options, callback) {
	var vertexId;
	const db = this.arango;

	// "vertex" is of format collection/id, this will return the id
	function idFromVertex(vertex) {
		const match = vertex.match(/^([^/]+)\/([^/]+)$/);

		return match && match[2];
	}

	try {
		const q = mongoAql.neighbors(graphName, vertex, edgeData, options);
		const cursor = await db.query(q.query, q.values);
		const data = await cursor.all();

		if (options.self && (vertexId = idFromVertex(vertex))) {
			data.push({ d: vertexId, v: 1, data: {} });
		}

		// delete metadata from "data", we don't need that
		data.forEach(function(el) {
			if (el.data) {
				delete el.data._key;
				delete el.data._id;
				delete el.data._from;
				delete el.data._to;
				delete el.data._rev;
			}
		});

		callback(null, data);
	}
	catch (err) {
		// not found
		if (err.errorNum === 1202) {
			callback(null, []);
		}
		else {
			return callback(error(err));
		}
	}
};

// edge can be null/undefined
SyncArango.prototype.getEdge = async function(graphName, from, to, edge, options, callback) {
	var vertexId;
	const db = this.arango;

	// "vertex" is of format collection/id, this will return the id
	function idFromVertex(vertex) {
		var match = vertex.match(/^([^/]+)\/([^/]+)$/);

		return match && match[2];
	}

	try {
		const q = mongoAql.edge(graphName, from, to, edge, options);
		const cursor = await db.query(q.query, q.values);
		const data = await cursor.all();

		// remove metadata (_to etc.) from the data.data properties
		if (data && data.length) {
			for (var i = 0; i < data.length; i++) {
				if (data[i].data) {
					for (j in data[i].data) {
						if (j.indexOf('_') === 0) {
							delete data[i].data[j];
						}
					}
				}
			}
		}

		callback(null, data);
	}

	catch (err) {
		if (err.errorNum === 1202) {
			callback(null, []);
		}
		else {
			return callback(error(err));
		}
	}
};

SyncArango.prototype.addEdge = async function(graphName, from, to, data, callback) {
	var edgeCollectionName;
	const db = this.arango;

	try {
		const res = await db.graph(graphName).get();

		// get the first edge collection - only one edge collection supported
		if (res && res.edgeDefinitions && res.edgeDefinitions && res.edgeDefinitions.length && res.edgeDefinitions[0] && res.edgeDefinitions[0].collection) {
			edgeCollectionName = res.edgeDefinitions[0].collection;
		}
		else {
			return callback('Edge definition not found.');
		}

		const edgeCollection = db.edgeCollection(edgeCollectionName);

		// check if there is already an edge
		// let there be only one edge (to make the connection unique)
		// we could do this with unique indexes but it would take more memory
		const doc = Object.assign({ _from: from, _to: to }, data);
		const cursor = await edgeCollection.byExample(doc);
		const results =	await cursor.all();

		if (results && results.length) {
			callback();
		}
		else {
			await edgeCollection.save(doc);
			callback();
		}
	}
	catch (err) {
		callback(error(err));
	}
}

SyncArango.prototype.removeEdge = async function(graphName, from, to, data, callback) {
	var edgeCollectionName;
	const db = this.arango;

	try {
		const res = await db.graph(graphName).get();

		// get the first edge collection - only one edge collection supported
		if (res && res.edgeDefinitions && res.edgeDefinitions && res.edgeDefinitions.length && res.edgeDefinitions[0] && res.edgeDefinitions[0].collection) {
			edgeCollectionName = res.edgeDefinitions[0].collection;
		}
		else {
			return callback('Edge definition not found.');
		}

		const edgeCollection = db.edgeCollection(edgeCollectionName);
		const doc = Object.assign({ _from: from, _to: to }, data);

		await edgeCollection.removeByExample(doc);
		callback();
	}
	catch (err) {
		callback(error(err));
	}
}

// We are removing a vertex from a document collection - this means
// that we want to remove all the edges that point to/from an edge collection
// so that there will be no orphaned edges.
SyncArango.prototype.removeVertex = async function(graphName, vertex, callback) {
	var edgeCollectionName;
	const db = this.arango;

	try {
		const res = await db.graph(graphName).get();

		// get the first edge collection - only one edge collection supported
		if (res && res.edgeDefinitions && res.edgeDefinitions && res.edgeDefinitions.length && res.edgeDefinitions[0] && res.edgeDefinitions[0].collection) {
			edgeCollectionName = res.edgeDefinitions[0].collection;
		}
		else {
			return callback('Edge definition not found.');
		}

		const edgeCollection = db.edgeCollection(edgeCollectionName);
		await edgeCollection.removeByExample({ _from: vertex });
		await edgeCollection.removeByExample({ _to: vertex });
	}
	catch (err) {
		callback(error(err));
	}			
}

SyncArango.prototype.setGraphData = async function(graphName, from, to, data, callback) {
	var edgeCollectionName;
	const db = this.arango;

	try {
		const res = await db.graph(graphName).get();

		// get the first edge collection - only one edge collection supported
		if (res && res.edgeDefinitions && res.edgeDefinitions && res.edgeDefinitions.length && res.edgeDefinitions[0] && res.edgeDefinitions[0].collection) {
			edgeCollectionName = res.edgeDefinitions[0].collection;
		}
		else {
			return callback('Edge definition not found.');
		}

		const edgeCollection = db.edgeCollection(edgeCollectionName);

		// check if there is already an edge
		// let there be only one edge (to make the connection unique)
		// we could do this with unique indexes but it would take more memory
		const doc = Object.assign({ _from: from, _to: to });
		await edgeCollection.updateByExample(doc, data);

		callback();
	}
	catch (err) {
		callback(error(err));
	}			
}

SyncArango.prototype.functionFetch = async function(aql, params, callback) {
	const db = this.arango;

	try {
		const cursor = await db.query(aql, params);
		const data = await cursor.all();

		for (var i = 0; i < data.length; i++) {
			data[i] = castFunctionFetchDataToSnapshot(data[i]);

		}

		callback(null, data);
	}
	catch (err) {
		callback(error(err, aql));
	}
}

function normalizeQuery(query) {
	// Deleted documents are kept around so that we can start their version from
	// the last version if they get recreated. Lack of a type indicates that a
	// snapshot is deleted, so don't return any documents with a null type

	if (Array.isArray(query)) {
		query = { _key: { $in: query } };
	}
	else if (typeof query !== 'object') {
		throw new Error('sync-arango: query not an array or an object' + query);
	}

	if (!query._type) {
		// create a clone of the object which we can then modify
		// and thus leave the original object intact
		query = Object.assign({}, query);
		query._type = { $ne: null };
	}

	return query;
}

function isObject(value) {
	return value !== null && typeof value === 'object' && !Array.isArray(value);
}

function castToDoc(id, snapshot, opLink) {
	var data = snapshot.data;
	var doc =
		(isObject(data)) ? shallowClone(data) :
		(data === undefined) ? {} :
		{_data: data};
	doc._key = id;
	doc._type = snapshot.type;
	doc._v = snapshot.v;
	doc._m = snapshot.m;
	doc._o = opLink;
	return doc;
}

function castToProjected(doc, projection) {
	if (projection && doc) {
		for (var i in doc) {
			if (!projection[i]) {
				delete doc[i];
			}
		}
	}

	return doc;
}

/*
** This works in two ways:
**  1) If called with getFunction as true then a function will be returned that
**     can be used for furher processing. The use case is for mapping a cursor.
**  2) if called with doc, projection only then normal casting is done.
*/

function castToProjectedSnapshotFunction(projection) {

	return function castToProjectedSnapshot(doc) {
		// are we checking for "existing" fields or "non-existing" fields
		// existing means the projection is of form { field1: 1, field2: 1 }
		// non-existing means the projection is of form { field1: 0, field2: 0 }
		// note that it should be either - or, it can't be mixed.
		var checkForExistingFields = false;

		doc = castToSnapshot(doc);

		// find out what type of projection we are using
		if (projection) {
			for (var i in projection) {
				if (projection[i]) {
					checkForExistingFields = true;
					break;
				}
			}
		}

		// how this works is:
		// if we are checking for "existing" fields, we will delete everything that isn't in projection
		// if we are checking for "non-existing" fields, we will delete everyting that is "0" in projection
		if (projection && doc && doc.data) {
			for (var i in doc.data) {
				if (checkForExistingFields && !projection[i]) {
					delete doc.data[i];
				}
				else if (!checkForExistingFields && projection[i] === 0) {
					delete doc.data[i];
				}
			}
		}

		return doc;
	}
}

function castToProjectedSnapshot(doc, projection) {
	return castToProjectedSnapshotFunction(projection)(doc);
}

function castFunctionFetchDataToSnapshot(doc) {
	var id = doc._key || uuid.v4();
	var version = doc._v || 1;
	var type = doc._type || "http://sharejs.org/types/JSONv0";
	var data = doc._data;
	var meta = doc._m;
	var opLink = doc._o;
	if (doc.hasOwnProperty('_data')) {
		return new ArangoSnapshot(id, version, type, data, meta, opLink);
	}
	var data = shallowClone(doc);
	delete data._id;
	delete data._key;
	delete data._v;
	delete data._type;
	delete data._m;
	delete data._o;
	return new ArangoSnapshot(id, version, type, data, meta, opLink);
}

function castToSnapshot(doc) {
	if (!doc) {
		return new ArangoSnapshot();	
	}

	var id = doc._key;
	var version = doc._v;
	var type = doc._type;
	var data = doc._data;
	var meta = doc._m;
	var opLink = doc._o;
	if (doc.hasOwnProperty('_data')) {
		return new ArangoSnapshot(id, version, type, data, meta, opLink);
	}
	var data = shallowClone(doc);
	delete data._id;
	delete data._key;
	delete data._v;
	delete data._type;
	delete data._m;
	delete data._o;
	return new ArangoSnapshot(id, version, type, data, meta, opLink);
}

function ArangoSnapshot(id, version, type, data, meta, opLink) {
	this.id = id;
	this.v = version;
	this.type = type;
	this.data = data;
	if (meta) this.m = meta;
	if (opLink) this._opLink = opLink;
}

function shallowClone(object) {
	var out = {};
	for (var key in object) {
		out[key] = object[key];
	}
	return out;
}

// Convert a simple map of fields that we want into a mongo projection. This
// depends on the data being stored at the top level of the document. It will
// only work properly for json documents--which are the only types for which
// we really want projections.
function getProjection(fields, options) {
	// Do not project when called by ShareDB submit
	if (fields === 'submit') return;
	// When there is no projection specified, still exclude returning the metadata
	// that is added to a doc for querying or auditing
	if (!fields) {
		return (options && options.metadata) ? {_o: 0} : {_m: 0, _o: 0};
	}
	if (fields.$submit) return;
	var projection = {};
	for (var key in fields) {
		projection[key] = 1;
	}
	projection._type = 1;
	projection._v = 1;
	if (options && options.metadata) projection._m = 1;
	return projection;
}

var metaOperators = {
	$comment: true
, $explain: true
, $hint: true
, $maxScan: true
, $max: true
, $min: true
, $orderby: true
, $returnKey: true
, $showDiskLoc: true
, $snapshot: true
, $count: true
, $aggregate: true
};

var cursorOperators = {
	$limit: 'limit'
, $skip: 'skip'
};

function error(err, param) {
	if (err)
	{
		console.log('[k-sync-arango]', err.toString());
		console.trace('[k-sync-arango] trace:');

		if (arguments.length > 1) {
			console.log('[k-sync-arango] params:');
			for (var i = 1; i < arguments.length; i++) {
				console.log(i+':', arguments[i]);
			}
		}

		return err.toString();
	}
}
