t=db.test
//for (var n=0;n<10;n++) {
	t.drop();
	t.insert({});
	for (var i=0;i<10;i++) {
		print(t.update({me:true},{me:true,count:i},{upsert:true,writeConcert:{w:0,j:false}}));
		t.find().forEach(printjson);
	}
//}
