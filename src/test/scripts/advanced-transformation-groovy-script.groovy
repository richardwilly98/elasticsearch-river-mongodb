def local = ctx.documents[0] 
ctx.documents = []
def doc1 = [operation: local.operation, _type: 'author', data: local.data.clone()] 
doc1.data.remove('tweets') 
ctx.documents << doc1 
for(tweet in local.data.tweets) { 
	ctx.documents << [operation: 'i', _parent: local.data._id, _type: 'tweet', data: tweet] 
}