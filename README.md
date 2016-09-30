# k-sync-arango

NOTE: this readme needs some work. 

ArangoDB database adapter for [k-sync](https://github.com/kantele/k-sync) (which is a fork of [k-sync](https://github.com/share/k-sync)). This
driver can be used both as a snapshot store and oplog.

Snapshots are stored where you'd expect (the named collection with _id=id). In
addition, operations are stored in `ops_COLLECTION`. For example, if you have
a `users` collection, the operations are stored in `ops_users`.

JSON document snapshots in k-sync-arango are unwrapped so you can use arango
queries directly against JSON documents. (They just have some extra fields in
the form of `_v` and `_type`). It is safe to query documents directly with the
ArangoDB driver or command line. Any read only arango features, including find,
aggregate, and map reduce are safe to perform concurrent with k-sync.

However, you must *always* use k-sync to edit documents. Never use the
ArangoDB driver or command line to directly modify any documents that k-sync
might create or edit. k-sync must be used to properly persist operations
together with snapshots.


## Usage

```js
var SyncArango = require('k-livedb-arango')('http://root:@localhost:8529/test');
var backend = kclient.createBackend({ db: SyncArango });
```

## Error codes

### todo - check these

arango errors are passed back directly. Additional error codes:

#### 4100 -- Bad request - DB

* 4101 -- Invalid op version
* 4102 -- Invalid collection name
* 4103 -- $where queries disabled
* 4104 -- $mapReduce queries disabled
* 4105 -- $aggregate queries disabled

#### 5100 -- Internal error - DB

* 5101 -- Already closed
* 5102 -- Snapshot missing last operation field
* 5103 -- Missing ops from requested version


## MIT License
Copyright (c) 2015 by Joseph Gentle and Nate Smith

Permission is hereby granted, free of charge, to any person obtaining a copy
of this software and associated documentation files (the "Software"), to deal
in the Software without restriction, including without limitation the rights
to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
copies of the Software, and to permit persons to whom the Software is
furnished to do so, subject to the following conditions:

The above copyright notice and this permission notice shall be included in
all copies or substantial portions of the Software.

THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN
THE SOFTWARE.
