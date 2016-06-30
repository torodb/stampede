init=function() {
	t=db.test
	t.drop();
}

if (mode == "init") {
	init();
} else {
	t=db.test
	for (var i=0;i<20;i++) {
		if (i%2==0) {
			u=t.update({me:true},{me:true,count:i},{upsert:true,writeConcert:{w:0,j:false}});
		} else {
			u=t.remove({me:true});
		}
		print("[thread-" + thread + "] " + u);
		assert.eq(false , u.hasWriteError(), "[thread-" + thread + "] " + u.getWriteError());
		t.find().forEach(function(j) { print("[thread-" + thread + "] " + tojson(j)); });
		assert.eq(false, t.find().count() > 1, "[thread-" + thread + "] found more than 1 documents");
	}
}