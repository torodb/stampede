t = db.binary
t.drop()

t.save( { a : BinData(0,"+QWU+Pwf1u03") } );
a=t.findOne();
assert.eq( 1 , a.a.base64() == BinData(0,"+QWU+Pwf1u03").base64(), "save" );

assert.eq( 1 , t.findOne({ a : BinData(0,"+QWU+Pwf1u03") }).a.base64() == BinData(0,"+QWU+Pwf1u03").base64(), "find" );

t.update({ _id: a._id }, { a : BinData(0,"+QWU+Pwf1u02") } );
a=t.findOne();
assert.eq( 1 , a.a.base64() == BinData(0,"+QWU+Pwf1u02").base64(), "update" );
