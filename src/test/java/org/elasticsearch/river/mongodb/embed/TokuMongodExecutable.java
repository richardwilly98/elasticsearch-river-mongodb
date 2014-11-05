package org.elasticsearch.river.mongodb.embed;

import java.io.IOException;

import de.flapdoodle.embed.mongo.MongodExecutable;
import de.flapdoodle.embed.mongo.MongodProcess;
import de.flapdoodle.embed.mongo.config.IMongodConfig;
import de.flapdoodle.embed.process.config.IRuntimeConfig;
import de.flapdoodle.embed.process.distribution.Distribution;
import de.flapdoodle.embed.process.extract.IExtractedFileSet;

public class TokuMongodExecutable extends MongodExecutable {

    public TokuMongodExecutable(Distribution distribution, IMongodConfig mongodConfig, IRuntimeConfig runtimeConfig, IExtractedFileSet files) {
        super(distribution, mongodConfig, runtimeConfig, files);
    }

    @Override
    protected MongodProcess start(Distribution distribution, IMongodConfig config, IRuntimeConfig runtime) throws IOException {
        return new TokuMongodProcess(distribution, config, runtime, this);
    }

}
