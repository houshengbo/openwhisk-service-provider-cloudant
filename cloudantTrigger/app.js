/**
 * Service which can be configured to listen for triggers from a cloudant
 * store and invoke POST whisk events appropriately.
 */
var _ = require('lodash');
var when = require('when');
var http = require('http');
var cradle = require('cradle');
var express = require('express');
var request = require('request');
var bodyParser = require('body-parser');
var logger = require('./Logger');

var tid = "??"; // TODO: tids
var routerHost = process.env.ROUTER_HOST || 'localhost';

// Limits of cloudant trigger
var triggersLimit = 10000;
var retriesBeforeDelete = 5;

// Allow invoking servers with self-signed certificates.
process.env.NODE_TLS_REJECT_UNAUTHORIZED = "0";

/*var cloudantUsername = process.env.CLOUDANT_USERNAME;
var cloudantPassword = process.env.CLOUDANT_PASSWORD;
var cloudantDbPrefix = process.env.DB_PREFIX;*/

var cloudantUsername = "houshengbo";
var cloudantPassword = "000000000";
var cloudantDbPrefix = "openwhisk";

var cloudantDatabase = cloudantDbPrefix + "cloudanttrigger";
var nano = require('nano')('https://' + cloudantUsername + ':' + cloudantPassword + '@' + cloudantUsername + '.cloudant.com');
nano.db.create(cloudantDatabase, function(err, body, header) {
    if (!err) {
        logger.info(tid, cloudantDatabase, ' database for cloudanttriggers was created.');
    }
});

var triggerDB = nano.db.use(cloudantDatabase);

// All environments
var app = express();

app.use(bodyParser.json());
app.use(bodyParser.urlencoded({ extended: false }));

app.set('port', process.env.PORT || 8080);

// Logging middleware
app.use(function(req, res, next) {
    if (req.url.indexOf('/cloudanttriggers') == 0)
        logger.info(tid, 'HttpRequest',req.method, req.url);
    next();
});

// RAS endpoint
app.get("/ping", function pong(req, res) {
    res.send({msg: 'pong'});
});

// Map of cloudant triggers.
// Trigger object has the following properties:
// - id: the id of this trigger
// - accounturl: the cloudant account url
// - dbname: the cloudant database
// - user: cloudant username
// - pass: cloudant password
// - since: emit events for changes since this time
// - callback: an object with one of the following:
//   - url: the callback url to POST to.
//   - action: an object with the following properties:
//     - name: the whisk action to invoke
// - feed: the feed object
var triggers = {};

// Creates a new trigger.
app.put('/cloudanttriggers/:id', function(req, res) {
    var method = 'PUT /cloudanttriggers';
    logger.info(tid, method);
    var args = typeof req.body === 'object' ? req.body : JSON.parse(req.body);
    if(args.maxTriggers > triggersLimit) {
        logger.warn(tid, method, 'maxTriggers > ' + triggersLimit + ' is not allowed');
        res.status(400).json({
            error: 'maxTriggers > ' + triggersLimit + ' is not allowed'
        });
        return;
    } else if (!args.callback || !args.callback.action || !args.callback.action.name) {
        logger.warn(tid, method, 'You callback is unknown for cloudant trigger:', args.callback);
        res.status(400).json({
            error: 'You callback is unknown for cloudant trigger.'
        });
        return;
    }
    var id = req.params.id;
    var trigger = initTrigger(args, id);
    // 10 is number of retries to create a trigger.
    var promise = createTrigger(trigger, 10);
    promise.then(function(newTrigger) {
        logger.info(tid, method, "Trigger was added and database is confirmed.", newTrigger);
        addTriggerToDB(newTrigger, res);
    }, function(err) {
        logger.error(tid, method, "Trigger could not be created.", err);
        deleteTrigger(id);
        res.status(400).json({
            message: "Trigger could not be created.",
            error: err
        });
    });
});

// Delete a trigger.
app.delete('/cloudanttriggers/:id', function(req, res) {
    var method = 'DELETE /cloudanttriggers';
    logger.info(tid, method);
    deleteTriggerFromDB(req.params.id, res);
});

function initTrigger(obj, id) {
    logger.info(tid, 'initTrigger', obj);
    var includeDoc = ((obj.includeDoc == true || obj.includeDoc.toString().trim().toLowerCase() == 'true')) || "false";
    var trigger = {
        id: id,
        accounturl: obj.accounturl,
        dbname: obj.dbname,
        user: obj.user,
        pass: obj.pass,
        includeDoc: includeDoc,
        apikey: obj.apikey,
        since: obj.since,
        callback: obj.callback,
        maxTriggers: obj.maxTriggers,
        triggersLeft: obj.maxTriggers,
        retriesLeft: retriesBeforeDelete
    };
    return trigger;
}

function initAllTriggers() {
    var method = 'initAllTriggers';
    logger.info(tid, method, 'Initializing all cloudant triggers from database.');
    triggerDB.list({include_docs: true}, function(err, body) {
        if(!err) {
            body.rows.forEach(function(trigger) {
                var cloudantTrigger = initTrigger(trigger.doc, trigger.doc.id);
                createTrigger(cloudantTrigger, 10);
            });
        } else {
            logger.error(tid, method, 'could not get latest state from database');
        }
    });
}

// Add a trigger: listen for changes and dispatch.
function createTrigger(dataTrigger, retryCount) {
    var method = 'createTrigger';
    // Listen for changes on this database.
    // TODO: Cleanup connection when trigger is deleted.
    var connection = new(cradle.Connection)(dataTrigger.accounturl, 443, {
        auth: {
            username: dataTrigger.user,
            password: dataTrigger.pass
        },
        cache: true,
        raw: false,
        forceSave: true
    });
    var db = connection.database(dataTrigger.dbname);
    var sinceToUse = dataTrigger.since ? dataTrigger.since : "now";
    var feed = db.changes({include_docs: dataTrigger.includeDoc, since: sinceToUse});

    dataTrigger.feed = feed;
    triggers[dataTrigger.id] = dataTrigger;

    feed.on('change', function (change) {
        var triggerHandle = triggers[dataTrigger.id];
        logger.info(tid, method, 'Got change from', dataTrigger.dbname, change);
        if(triggerHandle && triggerHandle.triggersLeft > 0 && triggerHandle.retriesLeft > 0) {
            try {
                invokeWhiskAction(dataTrigger.id, change);
            } catch (e) {
                logger.error(tid, method, 'Exception occurred in callback', e);
            }
        }
    });
    var promise = when.promise(function(resolve, reject) {
        feed.on('error', function (err) {
            logger.error(tid, method,'Error occurred for trigger', dataTrigger.id, '(db ' + dataTrigger.dbname + '):', err);
            // revive the feed if an error ocurred for now
            // the user should be in charge of removing the feeds
            logger.info(tid, "attempting to recreate trigger", dataTrigger.id);
            deleteTrigger(dataTrigger.id);
            dataTrigger.since = "now";
            if (retryCount > 0) {
                var addTriggerPromise = createTrigger(dataTrigger, (retryCount - 1));
                addTriggerPromise.then(function(trigger) {
                    logger.error(tid, method, "Retry Count:", (retryCount - 1));
                    resolve(trigger);
                }, function(err) {
                    reject(err);
                });
            } else {
                logger.error(tid, method, "Trigger's feed produced too many errors. Deleting the trigger", dataTrigger.id, '(db ' + dataTrigger.dbname + ')');
                reject({
                    error: err,
                    message: "Trigger's feed produced too many errors. Deleting the trigger " + dataTrigger.id
                });
            }
        });

        feed.on('confirm', function (dbObj) {
            logger.info(tid, method, 'Added cloudant data trigger', dataTrigger.id, 'listening for changes in database', dataTrigger.dbname);
            resolve(dataTrigger);
        });
    });

    return promise;
}

// Delete a trigger: stop listening for changes and remove it.
function deleteTrigger(id) {
    var method = 'deleteTrigger';
    var trigger = triggers[id];
    if (trigger) {
        logger.info(tid, method, 'Stopped cloudant trigger',
            id, 'listening for changes in database', trigger.dbname);
        trigger.feed.stop();
        delete triggers[id];
    } else {
        logger.info(tid, method, 'trigger', id, 'could not be found in the trigger list.');
        return false;
    }
}

function addTriggerToDB(trigger, res) {
    var method = 'addTriggerToDB';
    triggerDB.insert(_.omit(trigger, 'feed'), trigger.id, function(err, body) {
        if(!err) {
            logger.info(tid, method, 'trigger', trigger.id, 'was inserted into db.');
            res.status(200).json(_.omit(trigger, 'feed'));
        } else {
            deleteTrigger(trigger.id);
            res.status(err.statusCode).json({error: 'Cloudant trigger cannot be created.'});
        }
    });
}

function deleteTriggerFromDB(id, res) {
    var method = 'deleteTriggerFromDB';
    triggerDB.get(id, function(err, body) {
        if(!err) {
            triggerDB.destroy(body._id, body._rev, function(err, body) {
                if(err) {
                    logger.error(tid, method, 'there was an error while deleting', id, 'from database');
                    if (res) {
                        res.status(err.statusCode).json({ error: 'Cloudant data trigger ' + id  + 'cannot be deleted.' } );
                    }
                } else {
                    deleteTrigger(id);
                    logger.info(tid, method, 'cloudant trigger', id, ' is successfully deleted');
                    if (res) {
                        res.send('Deleted cloudant trigger ' + id);
                    }
                }
            });
        } else {
            if (err.statusCode === 404) {
                logger.info(tid, method, 'there was no trigger with id', id, 'in database.', err.error);
                if (res) {
                    res.status(200).json({ message: 'there was no trigger with id ' + id + ' in database.' } );
                    res.end();
                }
            } else {
                logger.error(tid, method, 'there was an error while getting', id, 'from database', err);
                if (res) {
                    res.status(err.statusCode).json({ error: 'Cloudant data trigger ' + id  + ' cannot be deleted.' } );
                }
            }
        }
    });
}

/**
 * Fire the whisk trigger
 */
function invokeWhiskAction(id, change) {
    var method = 'invokeWhiskAction';

    var dataTrigger = triggers[id];
    var apikey = dataTrigger.apikey;
    var triggerName = dataTrigger.callback.action.name;
    var triggerObj = parseQName(triggerName);
    logger.info(tid, method, 'invokeWhiskAction: change =', change);
    var form = change.hasOwnProperty('doc') ? change.doc : change;
    logger.info(tid, method, 'invokeWhiskAction: form =', form);
    logger.info(tid, method, 'for trigger', id, 'invoking action', triggerName, 'with db update', JSON.stringify(form));

    var host = 'https://'+routerHost+':'+443;
    var uri = host+'/api/v1/namespaces/' + triggerObj.namespace +'/triggers/'+triggerObj.name;
    var auth = apikey.split(':');
    logger.info(tid, method, uri, auth, form);

    dataTrigger.triggersLeft--;

    request({
        method: 'post',
        uri: uri,
        auth: {
            user: auth[0],
            pass: auth[1]
        },
        json: form
    }, function(error, response, body) {
        if(dataTrigger) {
            logger.info(tid, method, 'done http request, STATUS', response ? response.statusCode : response);
            logger.info(tid, method, 'done http request, body', body);
            if(error || response.statusCode >= 400) {
                dataTrigger.retriesLeft--;
                dataTrigger.triggersLeft++; // setting the counter back to where it used to be
                logger.error(tid, method, 'there was an error invoking', id, response ? response.statusCode : response, error, body);
            } else {
                dataTrigger.retriesLeft = retriesBeforeDelete; // reset retry counter
                logger.info(tid, method, 'fired', id, 'with body', body, dataTrigger.triggersLeft, 'triggers left');
            }

            if(dataTrigger.triggersLeft === 0 || dataTrigger.retriesLeft === 0) {
                if(dataTrigger.triggersLeft === 0)
                    logger.info(tid, 'onTick', 'no more triggers left, deleting');
                if(dataTrigger.retriesLeft === 0)
                    logger.info(tid, 'onTick', 'too many retries, deleting');

                deleteTriggerFromDB(dataTrigger.id);
            }
        } else {
            logger.info(tid, method, 'trigger', id, 'was deleted between invocations');
        }
    });
}

function parseQName(qname) {
    var parsed = {};
    var delimiter = '/';
    var defaultNamespace = '_';
    if (qname && qname.charAt(0) === delimiter) {
        var parts = qname.split(delimiter);
        parsed.namespace = parts[1];
        parsed.name = parts.length > 2 ? parts.slice(2).join(delimiter) : '';
    } else {
        parsed.namespace = defaultNamespace;
        parsed.name = qname;
    }
    return parsed;
}

var server = http.createServer(app);
server.listen(app.get('port'), function(){
    logger.info(tid, 'init', 'Express server listening on port ' + app.get('port'));
});

// Initialize global data structures
function init(server) {
    if (server != null) {
        var address = server.address();
        if (address == null) {
            logger.error(tid, 'init', 'Error initializing server. Perhaps port is already in use.');
            process.exit(-1);
        }
    }
    initAllTriggers();
}

init(server);