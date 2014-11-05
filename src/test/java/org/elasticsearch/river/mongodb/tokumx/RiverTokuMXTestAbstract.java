package org.elasticsearch.river.mongodb.tokumx;

import org.elasticsearch.river.mongodb.RiverMongoDBTestAbstract;

public abstract class RiverTokuMXTestAbstract extends RiverMongoDBTestAbstract {

    protected RiverTokuMXTestAbstract() {
        super(ExecutableType.TOKUMX);
    }

}
