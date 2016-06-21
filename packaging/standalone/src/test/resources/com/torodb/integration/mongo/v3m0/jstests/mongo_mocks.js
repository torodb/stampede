updateArgs = function(args) {
	if (args[0] == 'mongo') {
		var hasDb = false;
		for(var i = 1; i< args.length; i++) {
			if (args[i].indexOf('-') == 0) {
				i++;
			} else {
				if (args[i].indexOf('/') > -1) {
					args[i] = args[i].substring(0, args[i].indexOf('/') + 1) + db.getName();
					hasDb = true;
				} else
				if (args[i].indexOf(':') > -1) {
					args[i] = args[i] + '/' + db.getName();
					hasDb = true;
				} else {
					args[i] = db.getName();
					hasDb = true;
				}
				break;
			}
		}
		
		if (!hasDb) {
			args.push(db.getName());
		}
	} else
	if (["mongodump", "mongorestore", "mongoexport", "mongoimport"].indexOf(args[0]) > -1) {
		var hasDb = false;
		for(var i = 1; i< args.length; i++) {
			if (['--db', '-d'].indexOf(args[i]) > -1) {
				args[i+1] = db.getName();
				hasDb = true;
				break;
			}
		}
		
		if (!hasDb) {
			args.push('--db');
			args.push(db.getName());
		}
	}
}

_originalStartMongoProgram = _startMongoProgram;
_startMongoProgram = function() {
	var args = argumentsToArray(arguments);
	if (args[0] == 'mongod') {
		db.getCollectionNames().forEach(function(cn) { db.getCollection(cn).drop(); });
		return -1;
	}
	
	updateArgs(args)
	
	return _originalStartMongoProgram.apply(null, args);
};
//runProgram = function() { };
//run = function() { };
_originalRunMongoProgram = _runMongoProgram;
_runMongoProgram = function() {
	var args = argumentsToArray(arguments);
	
	if (args[0] == 'mongod')
		return -1;
	
	updateArgs(args)
	
	return _originalRunMongoProgram.apply(null, args);
};
stopMongod = function() {};
//stopMongoProgram = function() {};
//stopMongoProgramByPid = function() {};
//rawMongoProgramOutput = function() {};
//clearRawMongoProgramOutput = function() {};
originalWaitProgram = waitProgram;
waitProgram = function(pid) {
	if (pid == -1)
		return true;
	return originalWaitProgram(pid);
};
originalCheckProgram = checkProgram;
checkProgram = function(pid) {
	if (pid == -1)
		return true;
	return originalCheckProgram(pid);
};
//resetDbpath = function() {};
//pathExists = function() {};
copyDbpath = function() {};
Mongo = function() { this.getDB = function() { return db; }; this.forceWriteMode = function() {}; };
var OriginalToolTest = ToolTest;
ToolTest = function(name, extraOptions) {
	Object.extend(this, new OriginalToolTest(name, extraOptions));
	this.baseName = db.getName();
	this.port = 27018;
}
MongoRunner.dataDir = "/tmp/data/db"
MongoRunner.dataPath = "/tmp/data/db/"
MongoRunner.nextOpenPort = function() { return 27018; }
db.getSiblingDB = function() { return db; }
