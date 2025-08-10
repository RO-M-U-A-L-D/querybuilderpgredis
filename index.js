// Military-Grade Redis + PostgreSQL Integration Module
const Redis = require('redis');
const Pg = require('pg');

const CANSTATS = global.F ? (global.F.stats && global.F.stats.performance && global.F.stats.performance.dbrm != null) : false;
const REG_PG_ESCAPE_1 = /'/g;
const REG_PG_ESCAPE_2 = /\\/g;
const REG_LANGUAGE = /[a-z0-9]+§/gi;
const REG_WRITE = /(INSERT|UPDATE|DELETE|DROP|TRUNCATE)/i;
const REG_COL_TEST = /"|\s|:|\./;
const LOGGER = '-- PG+Redis -->';

// Global caches and pools
const POOLS = {};
const REDIS_POOLS = {};
const CIRCUIT_BREAKERS = {};
var FieldsCache = {};

// Cache configuration
const CACHE_CONFIG = {
	defaultTTL: 300, // 5 minutes
	maxTTL: 3600,    // 1 hour
	keyPrefix: 'pgcache:',
	compressionThreshold: 1024, // bytes
	maxRetries: 3,
	retryDelay: 100,
	circuitBreakerThreshold: 5,
	circuitBreakerTimeout: 30000
};

// Cache key generation
function generateCacheKey(filter, exec) {
	const keyParts = [
		exec || filter.exec,
		filter.table,
		filter.schema || 'default',
		JSON.stringify(filter.filter || []),
		JSON.stringify(filter.sort || []),
		JSON.stringify(filter.fields || []),
		filter.language || '',
		filter.take || '',
		filter.skip || '',
		filter.query || ''
	];
	
	const crypto = require('crypto');
	const hash = crypto.createHash('sha256').update(keyParts.join('|')).digest('hex');
	return CACHE_CONFIG.keyPrefix + hash;
}

// Circuit breaker implementation
class CircuitBreaker {
	constructor(name) {
		this.name = name;
		this.failures = 0;
		this.state = 'CLOSED'; // CLOSED, OPEN, HALF_OPEN
		this.nextAttempt = 0;
		this.threshold = CACHE_CONFIG.circuitBreakerThreshold;
		this.timeout = CACHE_CONFIG.circuitBreakerTimeout;
	}

	canExecute() {
		if (this.state === 'CLOSED') return true;
		if (this.state === 'OPEN') {
			if (Date.now() >= this.nextAttempt) {
				this.state = 'HALF_OPEN';
				return true;
			}
			return false;
		}
		if (this.state === 'HALF_OPEN') return true;
		return false;
	}

	onSuccess() {
		this.failures = 0;
		this.state = 'CLOSED';
	}

	onFailure() {
		this.failures++;
		if (this.failures >= this.threshold) {
			this.state = 'OPEN';
			this.nextAttempt = Date.now() + this.timeout;
		}
	}
}

// Redis operations with circuit breaker
class RedisManager {
	constructor(name, config) {
		this.name = name;
		this.config = config;
		this.client = null;
		this.breaker = new CircuitBreaker(`redis_${name}`);
		this.connecting = false;
		this.connected = false;
		this.init();
	}

	async init() {
		try {
			if (this.connecting) return;
			this.connecting = true;

			this.client = Redis.createClient(this.config);
			
			this.client.on('error', (err) => {
				console.error(`${LOGGER} Redis error (${this.name}):`, err.message);
				this.connected = false;
				this.breaker.onFailure();
			});

			this.client.on('connect', () => {
				console.log(`${LOGGER} Redis connected (${this.name})`);
				this.connected = true;
				this.breaker.onSuccess();
			});

			this.client.on('ready', () => {
				this.connected = true;
			});

			this.client.on('end', () => {
				this.connected = false;
			});

			await this.client.connect();
			this.connecting = false;
		} catch (err) {
			console.error(`${LOGGER} Redis init failed (${this.name}):`, err.message);
			this.connecting = false;
			this.connected = false;
			this.breaker.onFailure();
		}
	}

	async get(key) {
		if (!this.canExecute()) return null;
		
		try {
			const result = await this.executeWithRetry(async () => {
				return await this.client.get(key);
			});
			
			if (result) {
				this.breaker.onSuccess();
				try {
					return JSON.parse(result);
				} catch (parseErr) {
					return result;
				}
			}
			return null;
		} catch (err) {
			this.breaker.onFailure();
			console.warn(`${LOGGER} Redis GET failed:`, err.message);
			return null;
		}
	}

	async set(key, value, ttl = CACHE_CONFIG.defaultTTL) {
		if (!this.canExecute()) return false;
		
		try {
			let serialized;
			if (typeof value === 'object') {
				serialized = JSON.stringify(value);
			} else {
				serialized = String(value);
			}

			await this.executeWithRetry(async () => {
				if (ttl > 0) {
					return await this.client.setEx(key, ttl, serialized);
				} else {
					return await this.client.set(key, serialized);
				}
			});

			this.breaker.onSuccess();
			return true;
		} catch (err) {
			this.breaker.onFailure();
			console.warn(`${LOGGER} Redis SET failed:`, err.message);
			return false;
		}
	}

	async del(key) {
		if (!this.canExecute()) return false;
		
		try {
			await this.executeWithRetry(async () => {
				return await this.client.del(key);
			});
			this.breaker.onSuccess();
			return true;
		} catch (err) {
			this.breaker.onFailure();
			console.warn(`${LOGGER} Redis DEL failed:`, err.message);
			return false;
		}
	}

	async flush() {
		if (!this.canExecute()) return false;
		
		try {
			const keys = await this.executeWithRetry(async () => {
				return await this.client.keys(CACHE_CONFIG.keyPrefix + '*');
			});
			
			if (keys && keys.length > 0) {
				await this.executeWithRetry(async () => {
					return await this.client.del(keys);
				});
			}
			
			this.breaker.onSuccess();
			return true;
		} catch (err) {
			this.breaker.onFailure();
			console.warn(`${LOGGER} Redis FLUSH failed:`, err.message);
			return false;
		}
	}

	canExecute() {
		return this.connected && this.breaker.canExecute();
	}

	async executeWithRetry(operation, retries = CACHE_CONFIG.maxRetries) {
		let lastError;
		
		for (let attempt = 0; attempt <= retries; attempt++) {
			try {
				return await operation();
			} catch (err) {
				lastError = err;
				if (attempt < retries) {
					await this.delay(CACHE_CONFIG.retryDelay * (attempt + 1));
				}
			}
		}
		throw lastError;
	}

	delay(ms) {
		return new Promise(resolve => setTimeout(resolve, ms));
	}

	async close() {
		if (this.client && this.connected) {
			await this.client.disconnect();
		}
	}
}

// Cache-aware database executor
function execWithCache(name, client, filter, callback, done, errorhandling) {
	const redis = REDIS_POOLS[name];
	const isReadOperation = !REG_WRITE.test(filter.exec);
	const cacheKey = isReadOperation ? generateCacheKey(filter) : null;
	
	// For write operations, execute immediately and invalidate cache
	if (!isReadOperation) {
		executeAndInvalidate(name, client, filter, callback, done, errorhandling);
		return;
	}
	
	// For read operations, try cache first
	if (redis && cacheKey) {
		redis.get(cacheKey).then(cachedResult => {
			if (cachedResult !== null) {
				if (filter.debug) {
					console.log(`${LOGGER} Cache HIT: ${cacheKey}`);
				}
				done();
				callback(null, cachedResult);
				return;
			}
			
			// Cache miss - execute query and cache result
			executeAndCache(name, client, filter, callback, done, errorhandling, cacheKey);
		}).catch(err => {
			console.warn(`${LOGGER} Cache error, falling back to DB:`, err.message);
			executeAndCache(name, client, filter, callback, done, errorhandling, cacheKey);
		});
	} else {
		// No cache available - execute directly
		exec(client, filter, callback, done, errorhandling);
	}
}

function executeAndCache(name, client, filter, callback, done, errorhandling, cacheKey) {
	exec(client, filter, (err, result) => {
		if (!err && result && cacheKey) {
			const redis = REDIS_POOLS[name];
			if (redis) {
				const ttl = calculateTTL(filter);
				redis.set(cacheKey, result, ttl).catch(cacheErr => {
					console.warn(`${LOGGER} Failed to cache result:`, cacheErr.message);
				});
				
				if (filter.debug) {
					console.log(`${LOGGER} Cache SET: ${cacheKey} (TTL: ${ttl}s)`);
				}
			}
		}
		callback(err, result);
	}, done, errorhandling);
}

function executeAndInvalidate(name, client, filter, callback, done, errorhandling) {
	exec(client, filter, (err, result) => {
		if (!err) {
			// Invalidate related cache entries
			const redis = REDIS_POOLS[name];
			if (redis) {
				invalidateTableCache(redis, filter.table, filter.schema).catch(invalidateErr => {
					console.warn(`${LOGGER} Cache invalidation failed:`, invalidateErr.message);
				});
			}
		}
		callback(err, result);
	}, done, errorhandling);
}

async function invalidateTableCache(redis, table, schema) {
	try {
		const pattern = CACHE_CONFIG.keyPrefix + '*';
		const keys = await redis.client.keys(pattern);
		
		if (keys && keys.length > 0) {
			// Filter keys that might be related to this table
			const relatedKeys = keys.filter(key => {
				// This is a simplified check - you might want to make it more sophisticated
				return key.includes(table) || (schema && key.includes(schema));
			});
			
			if (relatedKeys.length > 0) {
				await redis.client.del(relatedKeys);
				console.log(`${LOGGER} Invalidated ${relatedKeys.length} cache entries for table ${schema ? schema + '.' : ''}${table}`);
			}
		}
	} catch (err) {
		throw err;
	}
}

function calculateTTL(filter) {
	let ttl = CACHE_CONFIG.defaultTTL;
	
	// Adjust TTL based on operation type
	switch (filter.exec) {
		case 'count':
		case 'scalar':
			ttl = CACHE_CONFIG.defaultTTL * 2; // Aggregate queries can be cached longer
			break;
		case 'find':
		case 'read':
			if (filter.take && filter.take <= 10) {
				ttl = CACHE_CONFIG.defaultTTL * 3; // Small result sets can be cached longer
			}
			break;
		case 'list':
			ttl = Math.max(60, CACHE_CONFIG.defaultTTL / 2); // Lists change more frequently
			break;
	}
	
	return Math.min(ttl, CACHE_CONFIG.maxTTL);
}

// Original exec function (unchanged for compatibility)
function exec(client, filter, callback, done, errorhandling) {
	var cmd;

	if (filter.exec === 'list') {
		try {
			cmd = makesql(filter);
		} catch (e) {
			done();
			callback(e);
			return;
		}

		if (filter.debug)
			console.log(LOGGER, cmd.query, cmd.params);

		client.query(cmd.query, cmd.params, function(err, response) {
			if (err) {
				done();
				errorhandling && errorhandling(err, cmd);
				callback(err);
			} else {
				cmd = makesql(filter, 'count');

				if (filter.debug)
					console.log(LOGGER, cmd.query, cmd.params);

				client.query(cmd.query, cmd.params, function(err, counter) {
					done();
					err && errorhandling && errorhandling(err, cmd);
					callback(err, err ? null : { items: response.rows, count: +counter.rows[0].count });
				});
			}
		});
		return;
	}

	try {
		cmd = makesql(filter);
	} catch (e) {
		done();
		callback(e);
		return;
	}

	if (filter.debug)
		console.log(LOGGER, cmd.query, cmd.params);

	client.query(cmd.query, cmd.params, function(err, response) {
		done();

		if (err) {
			errorhandling && errorhandling(err, cmd);
			callback(err);
			return;
		}

		var output;

		switch (filter.exec) {
			case 'insert':
				if (filter.returning)
					output = response.rows.length && response.rows[0];
				else if (filter.primarykey)
					output = response.rows.length && response.rows[0][filter.primarykey];
				else
					output = response.rowCount;
				callback(null, output);
				break;
			case 'update':
				if (filter.returning)
					output = filter.first ? (response.rows.length && response.rows[0]) : response.rows;
				else
					output = (response.rows.length && response.rows[0].count) || 0;
				callback(null, output);
				break;
			case 'remove':
				if (filter.returning)
					output = filter.first ? (response.rows.length && response.rows[0]) : response.rows;
				else
					output = response.rowCount;
				callback(null, output);
				break;
			case 'check':
				output = response.rows[0] ? response.rows[0].count > 0 : false;
				callback(null, output);
				break;
			case 'count':
				output = response.rows[0] ? response.rows[0].count : null;
				callback(null, output);
				break;
			case 'scalar':
				output = filter.scalar.type === 'group' ? response.rows : (response.rows[0] ? response.rows[0].value : null);
				callback(null, output);
				break;
			default:
				output = response.rows;
				callback(err, output);
				break;
		}
	});
}

// All original helper functions (unchanged for compatibility)
function pg_where(where, opt, filter, operator) {
	var tmp;

	for (var item of filter) {
		var name = '';

		if (item.name) {
			let key = 'where_' + (opt.language || '') + '_' + item.name;
			name = FieldsCache[key];

			if (!name) {
				name = item.name;
				if (name[name.length - 1] === '§')
					name = replacelanguage(item.name, opt.language, true);
				else
					name = REG_COL_TEST.test(item.name) ? item.name : ('"' + item.name + '"');
				FieldsCache[key] = name;
			}
		}

		switch (item.type) {
			case 'or':
				tmp = [];
				pg_where(tmp, opt, item.value, 'OR');
				where.length && where.push(operator);
				where.push('(' + tmp.join(' ') + ')');
				break;
			case 'in':
			case 'notin':
				where.length && where.push(operator);
				tmp = [];
				if (item.value instanceof Array) {
					for (var val of item.value) {
						if (val != null)
							tmp.push(PG_ESCAPE(val));
					}
				} else if (item.value != null)
					tmp = [PG_ESCAPE(item.value)];
				if (!tmp.length)
					tmp.push('null');
				where.push(name + (item.type === 'in' ? ' IN ' : ' NOT IN ') + '(' + tmp.join(',') + ')');
				break;			
			case 'array':
				where.length && where.push(operator);
				tmp = [];

				if (typeof(item.value) === 'string')
					item.value = item.value.split(',');

				for (let m of item.value)
					tmp.push(PG_ESCAPE(m));

				if (!tmp.length)
					tmp = ['\'\''];

				where.push(name + ' ' + item.comparer + ' ARRAY[' + tmp.join(',') + ']');
				break;
			case 'query':
				where.length && where.push(operator);
				where.push('(' + item.value + ')');
				break;
			case 'where':
				where.length && where.push(operator);
				if (item.value == null)
					where.push(name + (item.comparer === '=' ? ' IS NULL' : ' IS NOT NULL'));
				else
					where.push(name + item.comparer + PG_ESCAPE(item.value));
				break;
			case 'contains':
				where.length && where.push(operator);
				where.push('LENGTH(' + name +'::text)>0');
				break;
			case 'search':
				where.length && where.push(operator);

				tmp = item.value ? item.value.replace(/%/g, '') : '';

				if (item.operator === 'beg')
					where.push(name + ' ILIKE ' + PG_ESCAPE('%' + tmp));
				else if (item.operator === 'end')
					where.push(name + ' ILIKE ' + PG_ESCAPE(tmp + '%'));
				else
					where.push(name + '::text ILIKE ' + PG_ESCAPE('%' + tmp + '%'));
				break;
			case 'month':
			case 'year':
			case 'day':
			case 'hour':
			case 'minute':
				where.length && where.push(operator);
				where.push('EXTRACT(' + item.type + ' from ' + name + ')' + item.comparer + PG_ESCAPE(item.value));
				break;
			case 'empty':
				where.length && where.push(operator);
				where.push('(' + name + ' IS NULL OR LENGTH(' + name + '::text)=0)');
				break;
			case 'between':
				where.length && where.push(operator);
				where.push('(' + name + ' BETWEEN ' + PG_ESCAPE(item.a) + ' AND ' + PG_ESCAPE(item.b) + ')');
				break;
			case 'permit':
				where.length && where.push(operator);
				tmp = [];

				for (let m of item.value)
					tmp.push(PG_ESCAPE(m));

				if (!tmp.length)
					tmp = ['\'\''];

				if (item.required)
					where.push('(' + (item.userid ? ('userid=' + pg_escape(item.userid) + ' OR ') : '') + 'array_length(' + name + ',1) IS NULL OR ' + name + '::_text && ARRAY[' + tmp.join(',') + '])');
				else
					where.push('(' + (item.userid ? ('userid=' + pg_escape(item.userid) + ' OR ') : '') + name + '::_text && ARRAY[' + tmp.join(',') + '])');
				break;
		}
	}
}

function pg_insertupdate(filter, insert) {
	var query = [];
	var fields = insert ? [] : null;
	var params = [];

	for (var key in filter.payload) {
		var val = filter.payload[key];

		if (val === undefined)
			continue;

		var c = key[0];
		switch (c) {
			case '-':
			case '+':
			case '*':
			case '/':
				key = key.substring(1);
				params.push(val ? val : 0);
				if (insert) {
					fields.push('"' + key + '"');
					query.push('$' + params.length);
				} else
					query.push('"' + key + '"=COALESCE("' + key + '",0)' + c + '$' + params.length);
				break;
			case '>':
			case '<':
				key = key.substring(1);
				params.push(val ? val : 0);
				if (insert) {
					fields.push('"' + key + '"');
					query.push('$' + params.length);
				} else
					query.push('"' + key + '"=' + (c === '>' ? 'GREATEST' : 'LEAST') + '("' + key + '",$' + params.length + ')');
				break;
			case '!':
				key = key.substring(1);
				if (insert) {
					fields.push('"' + key + '"');
					query.push('FALSE');
				} else
					query.push('"' + key + '"=NOT ' + key);
				break;
			case '=':
			case '#':
				key = key.substring(1);
				if (insert) {
					if (c === '=') {
						fields.push('"' + key + '"');
						query.push(val);
					}
				} else
					query.push('"' + key + '"=' + val);
				break;
			default:
				params.push(val);
				if (insert) {
					fields.push('"' + key + '"');
					query.push('$' + params.length);
				} else
					query.push('"' + key + '"=$' + params.length);
				break;
		}
	}

	return { fields, query, params };
}

function replacelanguage(fields, language, noas) {
	return fields.replace(REG_LANGUAGE, function(val) {
		val = val.substring(0, val.length - 1);
		return '"' + val + '' + (noas ? ((language || '') + '"') : language ? (language + '" AS "' + val + '"') : '"');
	});
}

function makesql(opt, exec) {
	var query = '';
	var where = [];
	var model = {};
	var isread = false;
	var params;
	var returning;
	var tmp;

	if (!exec)
		exec = opt.exec;

	pg_where(where, opt, opt.filter, 'AND');

	var language = opt.language || '';
	var fields;
	var sort;

	if (opt.fields) {
		let key = 'fields_' + language + '_' + opt.fields.join(',');
		fields = FieldsCache[key] || '';
		if (!fields) {
			for (let i = 0; i < opt.fields.length; i++) {
				let m = opt.fields[i];
				if (m[m.length - 1] === '§')
					fields += (fields ? ',' : '') + replacelanguage(m, opt.language);
				else
					fields += (fields ? ',' : '') + (REG_COL_TEST.test(m) ? m : ('"' + m + '"'));
			}
			FieldsCache[key] = fields;
		}
	}

	switch (exec) {
		case 'find':
		case 'read':
			query = 'SELECT ' + (fields || '*') + ' FROM ' + opt.table2 + (where.length ? (' WHERE ' + where.join(' ')) : '');
			isread = true;
			break;
		case 'list':
			query = 'SELECT ' + (fields || '*') + ' FROM ' + opt.table2 + (where.length ? (' WHERE ' + where.join(' ')) : '');
			isread = true;
			break;
		case 'count':
			opt.first = true;
			query = 'SELECT COUNT(1)::int as count FROM ' + opt.table2 + (where.length ? (' WHERE ' + where.join(' ')) : '');
			isread = true;
			break;
		case 'insert':
			returning = opt.returning ? opt.returning.join(',') : opt.primarykey ? opt.primarykey : '';
			tmp = pg_insertupdate(opt, true);
			query = 'INSERT INTO ' + opt.table2 + ' (' + tmp.fields.join(',') + ') VALUES(' + tmp.query.join(',') + ')' + (returning ? ' RETURNING ' + returning : '');
			params = tmp.params;
			break;
		case 'remove':
			returning = opt.returning ? opt.returning.join(',') : opt.primarykey ? opt.primarykey : '';
			query = 'DELETE FROM ' + opt.table2 + (where.length ? (' WHERE ' + where.join(' ')) : '') + (returning ? ' RETURNING ' + returning : '');
			break;
		case 'update':
			returning = opt.returning ? opt.returning.join(',') : '';
			tmp = pg_insertupdate(opt);
			if (returning)
				query = 'UPDATE ' + opt.table2 + ' SET ' + tmp.query.join(',') + (where.length ? (' WHERE ' + where.join(' ')) : '') + (returning ? ' RETURNING ' + returning : '');
			else
				query = 'WITH rows AS (UPDATE ' + opt.table2 + ' SET ' + tmp.query.join(',') + (where.length ? (' WHERE ' + where.join(' ')) : '') + ' RETURNING 1) SELECT COUNT(1)::int count FROM rows';
			params = tmp.params;
			break;
		case 'check':
			query = 'SELECT 1 as count FROM ' + opt.table2 + (where.length ? (' WHERE ' + where.join(' ')) : '');
			isread = true;
			break;
		case 'drop':
			query = 'DROP TABLE ' + opt.table2;
			break;
		case 'truncate':
			query = 'TRUNCATE TABLE ' + opt.table2 + ' RESTART IDENTITY';
			break;
		case 'command':
			break;
		case 'scalar':
			switch (opt.scalar.type) {
				case 'avg':
				case 'min':
				case 'sum':
				case 'max':
				case 'count':
					opt.first = true;
					var val = opt.scalar.key === '*' ? 1 : opt.scalar.key;
					query = 'SELECT ' + opt.scalar.type.toUpperCase() + (opt.scalar.type !== 'count' ? ('(' + val + ')') : '(1)') + '::numeric as value FROM ' + opt.table2 + (where.length ? (' WHERE ' + where.join(' ')) : '');
					break;
				case 'group':
					query = 'SELECT ' + opt.scalar.key + ', ' + (opt.scalar.key2 ? ('SUM(' + opt.scalar.key2 + ')::numeric') : 'COUNT(1)::int') + ' as value FROM ' + opt.table2 + (where.length ? (' WHERE ' + where.join(' ')) : '') + ' GROUP BY ' + opt.scalar.key;
					break;
			}
			isread = true;
			break;
		case 'query':
			if (where.length) {
				let wherem = opt.query.match(/\{where\}/ig);
				let wherec = 'WHERE ' + where.join(' ');
				query = wherem ? opt.query.replace(wherem, wherec) : (opt.query + ' ' + wherec);
			} else
				query = opt.query;
			params = opt.params;
			isread = REG_WRITE.test(query) ? false : true;
			break;
	}

	if (exec === 'find' || exec === 'read' || exec === 'list' || exec === 'query' || exec === 'check') {
		if (opt.sort) {
			let key = 'sort_' + language + '_' + opt.sort.join(',');
			sort = FieldsCache[key] || '';
			if (!sort) {
				for (let i = 0; i < opt.sort.length; i++) {
					let m = opt.sort[i];
					let index = m.lastIndexOf('_');
					let name = m.substring(0, index);
					let value = (REG_COL_TEST.test(name) ? name : ('"' + name + '"')).replace(/§/, language);
					sort += (sort ? ',' : '') + value + ' ' + (m.substring(index + 1).toLowerCase() === 'desc' ? 'DESC' : 'ASC');
				}
				FieldsCache[key] = sort;
			}
			query += ' ORDER BY ' + sort;
		}

		if (opt.take && opt.skip)
			query += ' LIMIT ' + opt.take + ' OFFSET ' + opt.skip;
		else if (opt.take)
			query += ' LIMIT ' + opt.take;
		else if (opt.skip)
			query += ' OFFSET ' + opt.skip;
	}

	model.query = query;
	model.params = params;

	if (CANSTATS) {
		if (isread)
			F.stats.performance.dbrm++;
		else
			F.stats.performance.dbwm++;
	}

	return model;
}

function PG_ESCAPE(value) {
	if (value == null)
		return 'null';

	if (value instanceof Array) {
		var builder = [];
		if (value.length) {
			for (var m of value)
				builder.push(PG_ESCAPE(m));
			return 'ARRAY[' + builder.join(',') + ']';
		} else
			return 'null';
	}

	var type = typeof(value);

	if (type === 'function') {
		value = value();
		if (value == null)
			return 'null';
		type = typeof(value);
	}

	if (type === 'boolean')
		return value === true ? 'true' : 'false';

	if (type === 'number')
		return value + '';

	if (type === 'string')
		return pg_escape(value);

	if (value instanceof Date)
		return pg_escape(dateToString(value));

	if (type === 'object')
		return pg_escape(JSON.stringify(value));

	return pg_escape(value.toString());
}

// Author: https://github.com/segmentio/pg-escape
// License: MIT
function pg_escape(val) {
	if (val == null)
		return 'NULL';

	var backslash = ~val.indexOf('\\');
	var prefix = backslash ? 'E' : '';
	val = val.replace(REG_PG_ESCAPE_1, '\'\'').replace(REG_PG_ESCAPE_2, '\\\\');
	return prefix + '\'' + val + '\'';
}

function dateToString(dt) {
	var arr = [];

	arr.push(dt.getFullYear().toString());
	arr.push((dt.getMonth() + 1).toString());
	arr.push(dt.getDate().toString());
	arr.push(dt.getHours().toString());
	arr.push(dt.getMinutes().toString());
	arr.push(dt.getSeconds().toString());

	for (var i = 1; i < arr.length; i++) {
		if (arr[i].length === 1)
			arr[i] = '0' + arr[i];
	}

	return arr[0] + '-' + arr[1] + '-' + arr[2] + ' ' + arr[3] + ':' + arr[4] + ':' + arr[5];
}

// Set PostgreSQL type parser for numeric values
Pg.types.setTypeParser(1700, val => val == null ? null : +val);

// Global PG_ESCAPE for backward compatibility
global.PG_ESCAPE = PG_ESCAPE;

// Enhanced initialization with Redis support
exports.init = function(name, connstring, pooling, errorhandling, redisConfig) {
	if (!name)
		name = 'default';

	if (pooling)
		pooling = +pooling;

	// Clean up existing connections
	if (POOLS[name]) {
		POOLS[name].end();
		delete POOLS[name];
	}

	if (REDIS_POOLS[name]) {
		REDIS_POOLS[name].close();
		delete REDIS_POOLS[name];
	}

	if (!connstring) {
		// Remove instance
		NEWDB(name, null);
		return;
	}

	// Initialize Redis if configuration provided
	if (redisConfig) {
		try {
			REDIS_POOLS[name] = new RedisManager(name, redisConfig);
			console.log(`${LOGGER} Redis cache enabled for database "${name}"`);
		} catch (err) {
			console.error(`${LOGGER} Failed to initialize Redis for "${name}":`, err.message);
		}
	}

	var onerror = null;
	if (errorhandling) {
		onerror = (err, cmd) => {
			const errorMsg = err + (cmd ? (' - ' + cmd.query.substring(0, 100)) : '');
			errorhandling(errorMsg);
		};
	}

	var index = connstring.indexOf('?');
	var defschema = '';

	if (index !== -1) {
		var args = connstring.substring(index + 1).parseEncoded();
		defschema = args.schema;
		if (args.pooling)
			pooling = +args.pooling;
	}

	NEWDB(name, function(filter, callback) {
		if (filter.schema == null && defschema)
			filter.schema = defschema;

		filter.table2 = filter.schema ? (filter.schema + '.' + filter.table) : filter.table;

		if (pooling) {
			var pool = POOLS[name] || (POOLS[name] = new Pg.Pool({ 
				connectionString: connstring, 
				max: pooling,
				idleTimeoutMillis: 30000,
				connectionTimeoutMillis: 10000
			}));
			
			pool.connect(function(err, client, done) {
				if (err) {
					callback(err);
				} else {
					// Use cache-aware execution
					execWithCache(name, client, filter, callback, done, onerror);
				}
			});
		} else {
			var client = new Pg.Client({ connectionString: connstring });
			client.connect(function(err) {
				if (err) {
					callback(err);
				} else {
					// Use cache-aware execution
					execWithCache(name, client, filter, callback, () => client.end(), onerror);
				}
			});
		}
	});
};

// Cache management utilities
exports.cache = {
	// Flush all cache for a specific database instance
	flush: function(name) {
		name = name || 'default';
		const redis = REDIS_POOLS[name];
		if (redis) {
			return redis.flush();
		}
		return Promise.resolve(false);
	},

	// Flush cache for specific table
	flushTable: function(name, table, schema) {
		name = name || 'default';
		const redis = REDIS_POOLS[name];
		if (redis) {
			return invalidateTableCache(redis, table, schema);
		}
		return Promise.resolve(false);
	},

	// Get cache statistics
	stats: function(name) {
		name = name || 'default';
		const redis = REDIS_POOLS[name];
		if (redis) {
			return {
				connected: redis.connected,
				circuitBreakerState: redis.breaker.state,
				failures: redis.breaker.failures
			};
		}
		return null;
	},

	// Set custom cache key with TTL
	set: function(name, key, value, ttl) {
		name = name || 'default';
		const redis = REDIS_POOLS[name];
		if (redis) {
			return redis.set(CACHE_CONFIG.keyPrefix + 'custom:' + key, value, ttl);
		}
		return Promise.resolve(false);
	},

	// Get custom cache key
	get: function(name, key) {
		name = name || 'default';
		const redis = REDIS_POOLS[name];
		if (redis) {
			return redis.get(CACHE_CONFIG.keyPrefix + 'custom:' + key);
		}
		return Promise.resolve(null);
	},

	// Delete custom cache key
	del: function(name, key) {
		name = name || 'default';
		const redis = REDIS_POOLS[name];
		if (redis) {
			return redis.del(CACHE_CONFIG.keyPrefix + 'custom:' + key);
		}
		return Promise.resolve(false);
	}
};

// Configuration management
exports.config = function(options) {
	if (options.defaultTTL !== undefined)
		CACHE_CONFIG.defaultTTL = Math.max(1, +options.defaultTTL);
	
	if (options.maxTTL !== undefined)
		CACHE_CONFIG.maxTTL = Math.max(CACHE_CONFIG.defaultTTL, +options.maxTTL);
	
	if (options.keyPrefix !== undefined)
		CACHE_CONFIG.keyPrefix = String(options.keyPrefix);
	
	if (options.maxRetries !== undefined)
		CACHE_CONFIG.maxRetries = Math.max(0, +options.maxRetries);
	
	if (options.retryDelay !== undefined)
		CACHE_CONFIG.retryDelay = Math.max(10, +options.retryDelay);
	
	if (options.circuitBreakerThreshold !== undefined)
		CACHE_CONFIG.circuitBreakerThreshold = Math.max(1, +options.circuitBreakerThreshold);
	
	if (options.circuitBreakerTimeout !== undefined)
		CACHE_CONFIG.circuitBreakerTimeout = Math.max(1000, +options.circuitBreakerTimeout);

	return CACHE_CONFIG;
};

// Health check utility
exports.health = function(name) {
	name = name || 'default';
	const pool = POOLS[name];
	const redis = REDIS_POOLS[name];
	
	return {
		database: {
			connected: pool ? true : false,
			poolSize: pool ? pool.totalCount : 0,
			idleCount: pool ? pool.idleCount : 0,
			waitingCount: pool ? pool.waitingCount : 0
		},
		cache: redis ? {
			connected: redis.connected,
			circuitBreaker: {
				state: redis.breaker.state,
				failures: redis.breaker.failures,
				nextAttempt: redis.breaker.nextAttempt
			}
		} : null
	};
};

// Graceful shutdown
exports.close = async function(name) {
	if (name) {
		// Close specific instance
		if (POOLS[name]) {
			await POOLS[name].end();
			delete POOLS[name];
		}
		if (REDIS_POOLS[name]) {
			await REDIS_POOLS[name].close();
			delete REDIS_POOLS[name];
		}
	} else {
		// Close all instances
		const pgPromises = Object.values(POOLS).map(pool => pool.end());
		const redisPromises = Object.values(REDIS_POOLS).map(redis => redis.close());
		
		await Promise.allSettled([...pgPromises, ...redisPromises]);
		
		// Clear all pools
		Object.keys(POOLS).forEach(key => delete POOLS[key]);
		Object.keys(REDIS_POOLS).forEach(key => delete REDIS_POOLS[key]);
	}
};

// Service cleanup (Total.js integration)
ON('service', function(counter) {
	// Clear field cache periodically to prevent memory leaks
	if (counter % 10 === 0) {
		FieldsCache = {};
	}
	
	// Log health status every 100 service calls
	if (counter % 100 === 0) {
		Object.keys(POOLS).forEach(name => {
			const health = exports.health(name);
			if (health.cache && health.cache.circuitBreaker.state !== 'CLOSED') {
				console.warn(`${LOGGER} Cache circuit breaker for "${name}" is ${health.cache.circuitBreaker.state}`);
			}
		});
	}
});

// Advanced query execution with retry and fallback
exports.execute = async function(name, filter, options = {}) {
	return new Promise((resolve, reject) => {
		name = name || 'default';
		const db = global.DB ? global.DB(name) : null;
		
		if (!db) {
			reject(new Error(`Database instance "${name}" not found`));
			return;
		}
		
		// Apply options to filter
		if (options.cache === false) {
			filter.nocache = true;
		}
		if (options.debug === true) {
			filter.debug = true;
		}
		
		try {
			db(filter, (err, result) => {
				if (err) {
					reject(err);
				} else {
					resolve(result);
				}
			});
		} catch (err) {
			reject(err);
		}
	});
};

// Batch operations with automatic cache invalidation
exports.batch = async function(name, operations, options = {}) {
	name = name || 'default';
	const results = [];
	const errors = [];
	
	for (let i = 0; i < operations.length; i++) {
		try {
			const result = await exports.execute(name, operations[i], options);
			results.push({ index: i, success: true, result });
		} catch (error) {
			errors.push({ index: i, error: error.message });
			if (!options.continueOnError) {
				break;
			}
		}
	}
	
	return { results, errors, success: errors.length === 0 };
};
