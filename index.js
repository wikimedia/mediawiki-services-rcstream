/**
 * Broadcast MediaWiki recent changes over WebSockets
 *
 * To enable verbose debugging set environment `DEBUG=rcstream` or,
 * to include lower-level socket.io debug, `DEBUG=*`.
 *
 * @package node-rcstream
 */
var yargs = require('yargs');
var debug = require('debug');
var log = debug('rcstream:main');
var logRedis = debug('rcstream:redis');

var rcstream = (function () {
	var regexCache = {};
	var hasOwn = regexCache.hasOwnProperty;

	function escape(str) {
		return str.replace(/([.+?=^!:${}()|[\]\/\\])/g, '\\$1');
	}

	function makeMatchRegex(pattern) {
		if (!hasOwn.call(regexCache, pattern))
			regexCache[pattern] = new RegExp('^' + escape(pattern).replace(/\*/g, '.*') + '$');
		return regexCache[pattern];
	}

	/**
	 * Test a collection of wildcard patterns against a string.
	 *
	 * @param {string} str
	 * @param {string[]} patterns
	 */
	function matchAny(str, patterns) {
		for (var r, i = 0, len = patterns.length; i < len; i++) {
			r = makeMatchRegex(patterns[i]);
			if (r.test(str))
				return true;
		}
		return false;
	}

	/**
	 * Sort an array based on a computed sort key. Like python's array.sort(key=).
	 *
	 * @param {Array} list
	 * @param {Function} sortKey
	 */
	function keySort(list, sortKey) {
		list.sort(function (a, b) {
			return sortKey( a ) - sortKey( b );
		});
	}

	/**
	 * Sort key for subscriptions that puts shorter wildcard patterns ahead.
	 * NB: Contrary to python-rcstream#subscription_sort_key this always sorts
	 * wildcards before short ones, instead of treating them with equal priority.
	 *
	 * @return {number}
	 */
	function subSortKey(value) {
		return value.indexOf('*') !== -1 ? 0 : value.length;
	}

	return function (serverPort, redisHost, redisPort) {
		var MAX_SUBSCRIPTIONS = 10;
		var namespace = require('socket.io')(serverPort).of('/rc');
		var redisClient = require('redis').createClient(redisPort, redisHost);
		var cold = true;
		var subscriptionStore = {};

		log( 'binding to %d', serverPort);
		log( 'connect to redis at %s:%d', redisHost, redisPort);

		namespace.on('connect', function (socket) {
			log('socket %s connect', socket.id);
			var subscriptions = subscriptionStore[socket.id] = [];
			if (cold) {
				redisClient.psubscribe('rc.*');
				cold = false;
			}

			socket
			.on('subscribe', function (wikis) {
				var i, wiki;
				log('socket %s subscribe raw %j', socket.id, wikis);
				if (!Array.isArray(wikis))
					wikis = [wikis];
				i = wikis.length;
				while (i--) {
					wiki = wikis[i];
					if (typeof wiki !== 'string')
						continue;
					if (subscriptions.indexOf(wiki) !== -1)
						continue;
					if (subscriptions.length >= MAX_SUBSCRIPTIONS)
						socket.error('subscribe_error');

					log('socket %s subscribe add %j', socket.id, wiki);
					subscriptions.push(wiki);
					keySort(subscriptions, subSortKey);
				}
			})
			.on('unsubscribe', function (wikis) {
				var i, idx;
				if (!Array.isArray(wikis))
					wikis = [wikis];
				i = wikis.length;
				while (i--) {
					idx = subscriptions.indexOf(wikis[i]);
					if (idx !== -1)
						subscriptions.splice(idx, 1);
				}
			})
			.on('disconnect', function () {
				log('socket %s disconnect', socket.id);
				delete subscriptionStore[socket.id];
				subscriptions = undefined;
			});
		});

		redisClient.on('error', function () { logRedis('error %o', arguments); });
		redisClient.on('pmessage', function (pattern, channel, message) {
			var change, wiki, i, subscriptions;
			try {
				change = JSON.parse(message);
			} catch (e) {
				log('got invalid json from redis %o', message);
				return;
			}
			logRedis('pmessage %o', arguments);

			wiki = change.server_name;
			i = namespace.sockets.length;
			while (i--) {
				subscriptions = subscriptionStore[namespace.sockets[i].id];
				if (subscriptions && matchAny(wiki, subscriptions)) {
					namespace.sockets[i].emit('change', change);
				}
			}
		});
	};
}());

var arg = yargs.usage('Broadcast MediaWiki recent changes over WebSockets.\nUsage: $0')
    .option('help', {
    	desc: 'Show this help message and exit',
    	alias: 'h', boolean: true
    })
    .option('debug', {
    	desc: 'Enable debug output',
    	alias: ['v', 'verbose'], boolean: true,
    })
    .option('server-port', {
    	desc: 'Port for socket.io server to listen on',
    	default: 3000
    })
    .option('redis-host', {
    	desc: 'Host of Redis instance',
    	default: '127.0.0.1'
    })
    .option('redis-port', {
    	desc: 'Port of Redis instance',
    	default: 6379
    }).argv;

if (arg.help) {
	yargs.showHelp();
	return;
}

if (arg.verbose) {
	debug.enable('rcstream:*');
}

rcstream(arg['server-port'], arg['redis-host'], arg['redis-port']);
