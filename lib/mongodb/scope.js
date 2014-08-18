var Cursor2 = require('./cursor').Cursor
  , Readable = require('stream').Readable
  , utils = require('./utils')
  , inherits = require('util').inherits;

var Cursor = function Cursor(_scope_options, _cursor) {
  this._scope_options = _scope_options;
  this._cursor = _cursor;
};

//
// Backward compatible methods
Cursor.prototype.toArray = function(callback) {
  return this._cursor.toArray(callback);
};

Cursor.prototype.each = function(callback) {
  return this._cursor.each(callback);
};

Cursor.prototype.next = function(callback) {
  this.nextObject(callback);
};

Cursor.prototype.nextObject = function(callback) {
  return this._cursor.nextObject(callback);
};

Cursor.prototype.setReadPreference = function(readPreference, callback) {
  this._scope_options.readPreference = {readPreference: readPreference};
  this._cursor.setReadPreference(readPreference, callback);
  return this;
};

Cursor.prototype.batchSize = function(batchSize, callback) {
  this._scope_options.batchSize = batchSize;
  this._cursor.batchSize(this._scope_options.batchSize, callback);
  return this;
};

Cursor.prototype.count = function(applySkipLimit, callback) {
  return this._cursor.count(applySkipLimit, callback);
};

Cursor.prototype.stream = function(options) {
  return this._cursor.stream(options);
};

Cursor.prototype.close = function(callback) {
  return this._cursor.close(callback);
};

Cursor.prototype.explain = function(callback) {
  return this._cursor.explain(callback);
};

Cursor.prototype.isClosed = function(callback) {
  return this._cursor.isClosed();
};

Cursor.prototype.rewind = function() {
  return this._cursor.rewind();
};

// Internal methods
Cursor.prototype.limit = function(limit, callback) {
  this._cursor.limit(limit, callback);
  this._scope_options.limit = limit;
  return this;
};

Cursor.prototype.skip = function(skip, callback) {
  this._cursor.skip(skip, callback);
  this._scope_options.skip = skip;
  return this;
};

Cursor.prototype.hint = function(hint) {
  this._scope_options.hint = hint;
  this._cursor.hint = this._scope_options.hint;
  return this;
};

Cursor.prototype.maxTimeMS = function(maxTimeMS) {
  this._cursor.maxTimeMS(maxTimeMS)
  this._scope_options.maxTimeMS = maxTimeMS;
  return this;
};

Cursor.prototype.sort = function(keyOrList, direction, callback) {
  this._cursor.sort(keyOrList, direction, callback);
  this._scope_options.sort = keyOrList;
  return this;
};

Cursor.prototype.fields = function(fields) {
  this._cursor.fields = fields;
  return this;
};

//
// Backward compatible settings
Object.defineProperties(Cursor.prototype, {
  timeout: {
    get: function () {
      return this._cursor.timeout;
    }
  },
  items: {
    get: function () {
      return this._cursor.items;
    }
  },
  readPreference: {
    get: function () {
      return this._cursor.readPreference;
    }
  }
});

var Scope = function(collection, _selector, _fields, _scope_options) {
  var self = this;

  // Ensure we have at least an empty cursor options object
  _scope_options = _scope_options || {};
  var _write_concern = _scope_options.write_concern || null;

  // Ensure default read preference
  // if(!_scope_options.readPreference) _scope_options.readPreference = 'primary';

  // Set up the cursor
  var _cursor = new Cursor2(
        collection.db, collection, _selector
      , _fields, _scope_options
    );

  // Write branch options
  var writeOptions = {
    insert: function(documents, callback) {
      // Merge together options
      var options = _write_concern || {};
      // Execute insert
      collection.insert(documents, options, callback);
    },

    save: function(document, callback) {
      // Merge together options
      var save_options = _write_concern || {};
      // Execute save
      collection.save(document, save_options, function(err, result) {
        if(typeof result == 'number' && result == 1) {
          return callback(null, document);
        }

        return callback(null, document);
      });
    },

    find: function(selector) {
      _selector = selector;
      return writeOptions;
    },

    //
    // Update is implicit multiple document update
    update: function(operations, callback) {
      // Merge together options
      var update_options = _write_concern || {};

      // Set up options, multi is default operation
      update_options.multi = _scope_options.multi ? _scope_options.multi : true;
      if(_scope_options.upsert) update_options.upsert = _scope_options.upsert;

      // Execute options
      collection.update(_selector, operations, update_options, function(err, result, obj) {
        callback(err, obj);
      });
    },
  }

  // Set write concern
  this.withWriteConcern = function(write_concern) {
    // Save the current write concern to the Scope
    _scope_options.write_concern = write_concern;
    _write_concern = write_concern;
    // Only allow legal options
    return writeOptions;
  }

  // Start find
  this.find = function(selector, options) {
    // Save the current selector
    _selector = selector;
    // Set the cursor
    _cursor.selector = selector;
    // Return only legal read options
    return new Cursor(_scope_options, _cursor);
  }
}

exports.Scope = Scope;
