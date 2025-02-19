/**
 * Constructor for DataCache.
 * More info on caching: https://developers.google.com/apps-script/reference/cache/cache
 * source: https://gist.github.com/Bajena/db4296c7fa17642148e39187369dc911#file-datacache-gs
 *
 * @param {object} cacheService - GDS caching service
 *
 * @return {object} DataCache.
 */

function DataCache(cacheService, cacheKey) {
  this.service = cacheService;
  this.cacheKey = cacheKey;

  return this;
}

/** @const - 6 hours, Google's max */
DataCache.REQUEST_CACHING_TIME = 21600;

/** @const - 100 KB */
DataCache.MAX_CACHE_SIZE = 100 * 1024;

/**
 * Gets stored value
 *
 * @return {String} Response string
 */
DataCache.prototype.get = function() {
  var value = '';
  var chunk = '';
  var chunkIndex = 0;

  do {
    var chunkKey = this.getChunkKey(chunkIndex);
    chunk = this.service.get(chunkKey);
    value += (chunk || '');
    chunkIndex++;
  } while (chunk && chunk.length == DataCache.MAX_CACHE_SIZE);

  return value;
};

/**
 * Stores value in cache.
 *
 * @param {String} key - cache key
 * @param {String} value
 */
DataCache.prototype.set = function(value) {
  this.storeChunks(value);
};

DataCache.prototype.storeChunks = function(value) {
  var chunks = this.splitInChunks(value);

  for (var i = 0; i < chunks.length; i++) {
    var chunkKey = this.getChunkKey(i);
    this.service.put(chunkKey, chunks[i], DataCache.REQUEST_CACHING_TIME);
  }
};

DataCache.prototype.getChunkKey = function(chunkIndex) {
  return this.cacheKey + '_' + chunkIndex;
};

DataCache.prototype.splitInChunks = function(str) {
  var size = DataCache.MAX_CACHE_SIZE;
  var numChunks = Math.ceil(str.length / size);
  var chunks = new Array(numChunks);

  for (var i = 0, o = 0; i < numChunks; ++i, o += size) {
    chunks[i] = str.substr(o, size);
  }

  return chunks;
};