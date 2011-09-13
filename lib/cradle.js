var path = require('path');

var modpath = path.join(__dirname, 'cradle');

var sys = require("sys"),
   http = require("http"),
   https = require("https"),
 events = require('events'),
     fs = require("fs"),
    url = require('url'),
    http = require('http'),
    https = require('https'),
    querystring = require('querystring'),
    request = require('request');

var cradle = exports;

cradle.extend   = require(path.join(modpath, 'response')).extend;
cradle.Response = require(path.join(modpath, 'response')).Response;
cradle.Cache    = require(path.join(modpath, 'cache')).Cache;

cradle.host = '127.0.0.1';
cradle.port = 5984;
cradle.auth = null;
cradle.options = {
    cache: true,
    raw: false,
    timeout: 0,
    secure: false,
    headers: {}
};

cradle.setup = function (settings) {
    this.host = settings.host;
    this.auth = settings.auth;
    if (settings.port) {
      this.port = parseInt(settings.port, 10);
    }
    cradle.merge(this.options, settings);

    return this;
};

var protocolPattern = /^(https?):\/\//;

cradle.Connection = function Connection(/* variable args */) {
    var args = Array.prototype.slice.call(arguments),
        options = {},
        remote,
        match,
        host, 
        port, 
        auth;

    args.forEach(function (a) {
        if (typeof(a) === 'number' || (typeof(a) === 'string' && /^\d{2,5}$/.test(a))) {
            port = parseInt(a);
        } else if (typeof(a) === 'object') {
            options = a;
            host = host || options.host;
            port = port || options.port;
            auth = options.auth;
        } else {
            host = a;
            
            if (match = host.match(/^(.+)\:(\d{2,5})$/)) {
                host = match[1];
                port = parseInt(match[2]);
            }
        }
    });

    this.host    = host || cradle.host;
    this.port    = port || cradle.port;
    this.auth    = auth || cradle.auth;
    this.options = cradle.merge({}, cradle.options, options);

    this.options.maxSockets = this.options.maxSockets || 20;
    this.options.secure     = this.options.secure     || this.options.ssl;

    if (protocolPattern.test(this.host)) {
        this.protocol = this.host.match(protocolPattern)[1];
        this.host     = this.host.replace(protocolPattern, '');
    }

    if (this.protocol === 'https') this.options.secure = true;

    if (this.auth && this.auth.user) { // Deprecation warning
        console.log('Warning: "user" & "pass" parameters ignored. Use "username" & "password"');
    }
    if (this.options.ssl) { // Deprecation warning
        console.log('Warning: "ssl" option is deprecated. Use "secure" instead.');
    }

    this.transport = (this.options.secure) ? https : http;
    this.agent = new (this.transport.Agent)({
        host: this.host,
        port: this.port
    });
    
    this.agent.maxSockets = this.options.maxSockets;
};

//
// Connection.rawRequest()
//
//      This is a base wrapper around connections to CouchDB. Given that it handles
//      *all* requests, including those for attachments, it knows nothing about
//      JSON serialization and does not presuppose it is sending or receiving JSON
//      content
//
// OLDAPI: function (method, path, options, data, headers)
// 
cradle.Connection.prototype.rawRequest = function (options, callback) {
    var promise = new(events.EventEmitter), 
        self = this;

    // HTTP Headers
    options.headers = options.headers || {};

    // Set HTTP Basic Auth
    if (this.auth) {
        options.headers['Authorization'] = "Basic " + new Buffer(this.auth.username + ':' + this.auth.password).toString('base64');
    }

    // Set client-wide headers
    Object.keys(this.options.headers).forEach(function (header) {
        options.headers[header] = self.options.headers[header];
    });
            
    if (options.query && Object.keys(options.query).length) {
        for (var k in options.query) {
            if (typeof(options.query[k]) === 'boolean') {
                options.query[k] = String(options.query[k]);
            }
        }
        options.path += '?' + querystring.stringify(options.query);
    }

    options.headers['Connection'] = options.headers['Connection'] || 'keep-alive';
    options.agent = this.agent;
    options.uri = this._url(options.path);
    delete options.path;

    return request(options, callback || function () { });
};

//
// Connection.close()
//
//      Close all underlying sockets associated with the agent for the connection.
//
cradle.Connection.prototype.close = function () {
  var sockets = this.socket.globalAgent.sockets[this.host + ':' + this.port];
  sockets.forEach(function (socket) {
      socket.end();
  });
}

//
// Connection.request()
//
//      This is the entry point for all requests to CouchDB, at this point,
//      the database name has been embed in the url, by one of the wrappers.
//
cradle.Connection.prototype.request = function (options, callback) {
    var headers = cradle.merge({ host: this.host }, options.headers || {}),
        self = this;

    // HTTP Headers
    options.headers = options.headers || {};
    
    //
    // Handle POST/PUT data. We also convert functions to strings,
    // so they can be used in _design documents.
    //
    if (options.body) {
        options.body = JSON.stringify(options.body, function (k, val) {
            if (typeof(val) === 'function') {
                return val.toString();
            } else { return val }
        });
        options.headers["Content-Length"] = Buffer.byteLength(options.body);
        options.headers["Content-Type"]   = "application/json";
    }

    return this.rawRequest(options, function (err, res, body) {
        if (err) {
          return callback(err);
        }
        else if (options.method === 'HEAD') {
          return callback(null, res.headers, res.statusCode);
        }
        else if (body && body.error) {
          cradle.extend(body, { headers: res.headers });
          body.headers.status = res.statusCode;
          return callback(body);
        }
      
        try { body = JSON.parse(body) }
        catch (err) { }
      
        if (body && body.error) {
            cradle.extend(body, { headers: res.headers });
            body.headers.status = res.statusCode;
            return callback(body);
        }
      
        callback(null, self.options.raw ? body : new cradle.Response(body, res));
    });
};

//
// The database object
//
//      We return an object with database functions,
//      closing around the `name` argument.
//
cradle.Connection.prototype.database = function (name) {
    var that = this, connection = this;

    return {
        name: name,
        //
        // The database document cache.
        //
        cache: new(cradle.Cache)(that.options),

        // A wrapper around `Connection.request`,
        // which prepends the database name.
        query: function (method, path /* [options], [data], [headers], [callback] */) {
            var args = Array.prototype.slice.call(arguments, 2);
            that.request.apply(that, [method, [name, path].join('/')].concat(args));
        },
        exists: function (callback) {
            this.query('HEAD', '', function (err, res, status) {
                if (err) {
                    callback(err);
                } else {
                    if (status === 404) {
                        callback(null, false);
                    } else {
                        callback(null, true);
                    }
                }
            });
        },

        // Fetch either a single document from the database, or cache,
        // or multiple documents from the database.
        // If it's a single doc from the db, attempt to save it to the cache.
        get: function (id, rev) {
            var that = this, options = null,
                args = new(Args)(arguments);

            if (Array.isArray(id)) { // Bulk GET
                this.query('POST', '/_all_docs', { include_docs: true }, { keys: id },
                           function (err, res) { args.callback(err, res) });
            } else {
                if (rev && args.length === 2) {
                    if      (typeof(rev) === 'string') { options = { rev: rev } }
                    else if (typeof(rev) === 'object') { options = rev }
                } else if (this.cache.has(id)) {
                    return args.callback(null, this.cache.get(id));
                }
                this.query('GET', querystring.escape(id), options, function (err, res) {
                    if (! err) that.cache.save(res.id, res.json);
                    args.callback(err, res);
                });
            }
        },

        save: function (/* [id], [rev], doc | [doc, ...] */) {
            var args = new(Args)(arguments),
                array = args.all.slice(0), doc, id, rev;

            if (Array.isArray(args.first)) {
                doc = args.first;
            } else {
                doc = array.pop(),
                id  = array.shift(),
                rev = array.shift();
            }
            this._save(id, rev, doc, args.callback);
        },
       _save: function (id, rev, doc, callback) {
            var options = connection.options;
            var document = {}, that = this;

            // Bulk Insert
            if (Array.isArray(doc)) {
                document.docs = doc;
                if (options.allOrNothing) { document.all_or_nothing = true }
                this.query('POST', '/_bulk_docs', {}, document, callback);
            } else {
                // PUT a single document, with an id (Create or Update)
                if (id) {
                    // Design document
                    if (/^_design\/(\w|%|\-)+$/.test(id) && !('views' in doc)) {
                        document.language = "javascript";
                        document.views    =  doc;
                    } else {
                        document = doc;
                    }
                    // Try to set the '_rev' attribute of the document.
                    // If it wasn't passed, attempt to retrieve it from the cache.
                    rev && (document._rev = rev);

                    if (document._rev) {
                        this.put(id, document, callback);
                    } else if (this.cache.has(id)) {
                        document._rev = this.cache.get(id)._rev;
                        this.put(id, document, callback);
                    } else {
                        // Attempt to create a new document. If it fails,
                        // because an existing document with that _id exists (409),
                        // perform a HEAD, to get the _rev, and try to re-save.
                        this.put(id, document, function (e, res) {
                            if (e && e.headers && e.headers.status === 409) { // Conflict
                                that.head(id, function (e, headers) {
                                    document._rev = headers['etag'].slice(1, -1);
                                    that.put(id, document, callback);
                                });
                            } else { callback(e, res) }
                        });
                    }
                // POST a single document, without an id (Create)
                } else {
                    this.post(doc, callback);
                }
            }
        },

        merge: function (/* [id], doc */) {
            var args     = Array.prototype.slice.call(arguments),
                callback = args.pop(),
                doc      = args.pop(),
                id       = args.pop() || doc._id;

            this._merge(id, doc, callback);
        },
       _merge: function (id, doc, callback) {
            var that = this;
            this.get(id, function (e, res) {
                if (e) { return callback(e) }
                doc = cradle.merge({}, res.json || res, doc);
                that.save(id, res._rev, doc, callback);
            });
        },

        //
        // PUT a document, and write through cache
        //
        put: function (id, doc, callback) {
            var cache = this.cache;
            if (typeof(id) !== 'string') { throw new(TypeError)("id must be a string") }
            this.query('PUT', id, null, doc, function (e, res) {
                if (! e) { cache.save(id, cradle.merge({}, doc, { _id: id, _rev: res.rev })) }
                callback && callback(e, res);
            });
        },

        //
        // POST a document, and write through cache
        //
        post: function (doc, callback) {
            var cache = this.cache;
            this.query('POST', '/', null, doc, function (e, res) {
                if (! e) { cache.save(res.id, cradle.merge({}, doc, { _id: res.id, _rev: res.rev })) }
                callback && callback(e, res);
            });
        },

        //
        // Perform a HEAD request
        //
        head: function (id, callback) {
            this.query('HEAD', id.split('/').map(querystring.escape).join('/'), null, callback);
        },

        insert: function () {
            throw new(Error)("`insert` is deprecated, use `save` instead");
        },

        replicate: function (target, options, callback) {
            if (typeof(options) === 'function') { callback = options, options = {} }
            that.replicate(cradle.merge({ source: name, target: target }, options), callback);
        },

        // Destroys a database with 'DELETE'
        // we raise an exception if arguments were supplied,
        // as we don't want users to confuse this function with `remove`.
        destroy: function (callback) {
            if (arguments.length > 1) {
                throw new(Error)("destroy() doesn't take any additional arguments");
            } else {
                this.query('DELETE', '/', callback);
            }
        },

        // Delete a document
        // if the _rev wasn't supplied, we attempt to retrieve it from the
        // cache. If the deletion was successful, we purge the cache.
        remove: function (id, rev) {
            var that = this, doc, args = new(Args)(arguments);

            if (typeof(rev) !== 'string') {
                if (doc = this.cache.get(id)) { rev = doc._rev }
                else                          { throw new(Error)("rev needs to be supplied") }
            }
            this.query('DELETE', id.split('/').map(querystring.escape).join('/'), {rev: rev}, function (err, res) {
                if (! err) { that.cache.purge(id) }
                args.callback(err, res);
            });
        },
        create: function (callback) {
            this.query('PUT', '/', callback);
        },
        info: function (callback) {
            this.query('GET', '/', callback);
        },
        all: function (options, callback) {
            if (arguments.length === 1) { callback = options, options = {} }
            this.query('GET', '/_all_docs', options, callback);
        },
        compact: function (design) {
            var headers = {};
            headers['Content-Type'] = "application/json";
            this.query('POST', '/_compact' + (typeof(design) === 'string' ? '/' + querystring.escape(design) : ''),
                       {}, {}, headers, Args.last(arguments));
        },
        viewCleanup: function (callback) {
            var headers = {};
            headers['Content-Type'] = "application/json";
            this.query('POST', '/_view_cleanup', {}, {}, headers, callback);
        },
        allBySeq: function (options) {
            options = typeof(options) === 'object' ? options : {};
            this.query('GET', '/_all_docs_by_seq', options, Args.last(arguments));
        },

        // Query a view, passing any options to the query string.
        // Some query string parameters' values have to be JSON-encoded.
        view: function (path, options) {
            var args = new(Args)(arguments);

            path = path.split('/');
            path = ['_design', path[0], '_view', path[1]].map(querystring.escape).join('/');

            if (typeof(options) === 'object') {
                ['key', 'startkey', 'endkey'].forEach(function (k) {
                    if (k in options) { options[k] = JSON.stringify(options[k]) }
                });
            }

            if (options && options.keys) {
                this.query('POST', path, {}, options, args.callback);
            } else {
                this.query('GET', path, options, args.callback);
            }
        },

        // Query a list, passing any options to the query string.
        // Some query string parameters' values have to be JSON-encoded.
        list: function (path, options) {
            var args = new(Args)(arguments);
            path = path.split('/');

            if (typeof(options) === 'object') {
                ['key', 'startkey', 'endkey'].forEach(function (k) {
                    if (k in options) { options[k] = JSON.stringify(options[k]) }
                });
            }
            this.query('GET', ['_design', path[0], '_list', path[1], path[2]].map(querystring.escape).join('/'), options, args.callback);
        },

        update: function(path, id, options) {
            var args = new(Args)(arguments);
            path = path.split('/');

            if (id) {
              this.query('PUT', ['_design', path[0], '_update', path[1], id].map(querystring.escape).join('/'), options, args.callback);
            } else {
              this.query('POST', ['_design', path[0], '_update', path[1]].map(querystring.escape).join('/'), options, args.callback);
            }
        },

        push: function (doc) {},

        changes: function (options, callback) {
            var promise = new(events.EventEmitter);

            if (typeof(options) === 'function') { callback = options, options = {}; }

            if (callback) {
                this.query('GET', '_changes', options, callback);
            } else {
                options           = options           || {};
                options.feed      = options.feed      || 'continuous';
                options.heartbeat = options.heartbeat || 1000;

                that.rawRequest('GET', [name, '_changes'].join('/'), options).on('response', function (res) {
                    var response = new(events.EventEmitter), buffer = [];
                    res.setEncoding('utf8');

                    response.statusCode = res.statusCode;
                    response.headers    = res.headers;

                    promise.emit('response', response);

                    res.on('data', function (chunk) {
                        if (chunk.trim()) {
                            buffer.push(chunk);

                            if (chunk.indexOf('\n') !== -1) {
                                buffer.length && response.emit('data', JSON.parse(buffer.join('')));
                                buffer = [];
                            }
                        }
                    }).on('end', function () {
                        response.emit('end');
                    });
                });
                return promise;
            }
        },

        saveAttachment: function (/* id, [rev], attachmentName, contentType, dataOrStream */) {
            var doc, pathname, headers = {}, response, body = [], resHeaders, error, db = this;

            var args = new(Args)(arguments), params = args.all;

            if (typeof(args.first) === 'object') { throw new(TypeError)("first argument must be a document id") }

            var id = params.shift(),
                dataOrStream = params.pop(),
                contentType  = params.pop(),
                attachmentName = params.pop(),
                rev = params.pop();

            if (!rev && db.cache.has(id)) {
                doc = { rev: db.cache.get(id)._rev };
            } else if (rev) {
                doc = { rev: rev };
            } else {
                doc = {};
            }

            pathname = '/' + [name, id, attachmentName].map(querystring.escape).join('/');
            headers['Content-Type'] = contentType;

            that.rawRequest('PUT', pathname, doc, dataOrStream, headers)
                .on('response', function (res) { resHeaders = { status: res.statusCode } })
                .on('data', function (chunk) { body.push(chunk) })
                .on('end', function () {
                    response = JSON.parse(body.join(''));
                    response.headers = resHeaders;

                    if (response.headers.status == 201) {
                        if (db.cache.has(id)) {
                            cached = db.cache.store[id].document;
                            cached._rev = response.rev;
                            cached._attachments = cached._attachments || {};
                            cached._attachments[attachmentName] = { stub: true };
                        }
                        args.callback(null, response);
                    } else {
                        args.callback(response);
                    }
                });
        },

        getAttachment: function(docId, attachmentName) {
            var pathname, req;
            pathname = '/' + [name, docId, attachmentName].map(querystring.escape).join('/');
            return that.rawRequest('GET', pathname);
        },

        temporaryView: function (doc, callback) {
            this.query('POST', '_temp_view', null, doc, callback);
        }
    }

};

//
// Wrapper functions for the server API
//
cradle.Connection.prototype.databases = function (callback) {
    this.request({ path: '/_all_dbs' }, callback);
};
cradle.Connection.prototype.config = function (callback) {
    this.request({ path: '/_config' }, callback);
};
cradle.Connection.prototype.info = function (callback) {
    this.request({ path: '/' }, callback);
};
cradle.Connection.prototype.stats = function (callback) {
    this.request({ path: '/_stats' }, callback);
};
cradle.Connection.prototype.activeTasks = function (callback) {
    this.request({ path: '/_active_tasks' }, callback);
};
cradle.Connection.prototype.uuids = function (count, callback) {
    if (typeof(count) === 'function') { 
        callback = count; 
        count = null;
    }
    
    this.request({ 
        method: 'GET', 
        path: '/_uuids', 
        query: count ? { count: count } : {}
    }, callback);
};
cradle.Connection.prototype.replicate = function (options, callback) {
    this.request({
        method: 'POST', 
        path: '/_replicate', 
        query: options
    }, callback);
};

cradle.Connection.prototype._url = function (path) {
    var url = (this.protocol || 'http') + '://' + this.host;

    if (this.port !== 443 && this.port !== 80) {
      url += ':' + this.port;
    }
    
    url += path[0] === '/' ? path : ('/' + path);

    return url;
}

cradle.escape = function (id) {
    return ['_design', '_changes', '_temp_view'].indexOf(id.split('/')[0]) === -1
        ? querystring.escape(id)
        : id;
};

cradle.merge = function (target) {
    var objs = Array.prototype.slice.call(arguments, 1);
    objs.forEach(function (o) {
        Object.keys(o).forEach(function (attr) {
            if (! o.__lookupGetter__(attr)) {
                target[attr] = o[attr];
            }
        });
    });
    return target;
};
