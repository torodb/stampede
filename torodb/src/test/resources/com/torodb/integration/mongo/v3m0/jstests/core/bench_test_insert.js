(function() {
    "use strict";

    function testInsert(docs, writeCmd, wc) {
        var t = db.bench_test_insert
        t.drop()

        var benchArgs = { ops : [ { ns : t.getFullName() ,
                                op : "insert" ,
                                doc : docs,
                                writeCmd : writeCmd,
                                writeConcern : wc} ],
                      parallel : 2,
                      seconds : 1,
                      totals : true ,
                      host : db.getMongo().host }

        if (jsTest.options().auth) {
            benchArgs['db'] = 'admin'
            benchArgs['username'] = jsTest.options().adminUser
            benchArgs['password'] = jsTest.options().adminPassword
        }

        var res = benchRun(benchArgs)
        printjson(res)

        assert.gt(t.count(), 0)
        assert.eq(t.findOne({}, {_id:0}), docs[0])
    }

    var docs = []
    for (var i = 0; i < 100; i++) {
        docs.push( { x : 1 } )
    }

    testInsert(docs, false, {});
    testInsert(docs, true, {"writeConcern" : {"w" : "majority"}});
    testInsert(docs, true, {"writeConcern" : {"w" : 1, "j": false}});
    testInsert(docs, true, {"writeConcern" : {"j" : true}});
})();
