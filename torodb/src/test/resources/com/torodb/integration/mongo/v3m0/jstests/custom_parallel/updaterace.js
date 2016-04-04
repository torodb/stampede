init=function() {
	t=db.test
	t.drop();
	t.insert({me:true,count:-1});
}

if (mode == "init") {
	init();
} else {
	t=db.test
	for (var i=0;i<20;i++) {
		u=t.update({me:true},{me:true,count:i},{upsert:false,writeConcert:{w:0,j:false}});
		print("[thread-" + thread + "] " + u);
		assert.eq(false , u.hasWriteError(), "[thread-" + thread + "] " + u.getWriteError());
		assert.eq(false , u.nMatched>1, "[thread-" + thread + "] update found more that one documents");
		assert.eq(false , u.nMatched<1, "[thread-" + thread + "] update did not find any document");
		t.find().forEach(function(j) { print("[thread-" + thread + "] " + tojson(j)); });
		assert.eq(false, t.find().count() > 1, "[thread-" + thread + "] found more than 1 documents");
	}
}