t = db.binary
t.drop()

t.save( { a : 1 } );
t.save( { a : undefined } );
r=t.find({ a:undefined });
assert.eq( 1 , r.count() == 1);
