package org.elasticsearch.river.mongodb.tokumx;

import org.elasticsearch.river.mongodb.RiverMongoDBTestAbstract;
import org.testng.SkipException;
import org.testng.annotations.BeforeClass;

public abstract class RiverTokuMXTestAbstract extends RiverMongoDBTestAbstract {

    protected RiverTokuMXTestAbstract() {
        super(ExecutableType.TOKUMX);
    }

    @BeforeClass
    protected void checkEnvironment() {
      if (!tokuIsSupported()) {
        throw new SkipException("Skipping tests because running tests on environment not supported by TokuMX.");
      }
    }
    

}
