/**
 * Adapted from de.flapdoodle.embed.mongo.MongodStarter.
 * Ultimately, it would be best to migrate this into Flapdoodle's mongo.config project
 */
package org.elasticsearch.river.mongodb.embed;

import de.flapdoodle.embed.mongo.Command;
import de.flapdoodle.embed.mongo.MongodExecutable;
import de.flapdoodle.embed.mongo.MongodProcess;
import de.flapdoodle.embed.mongo.config.IMongodConfig;
import de.flapdoodle.embed.process.config.IRuntimeConfig;
import de.flapdoodle.embed.process.distribution.Distribution;
import de.flapdoodle.embed.process.extract.IExtractedFileSet;
import de.flapdoodle.embed.process.runtime.Starter;

public class TokuMXStarter extends Starter<IMongodConfig, MongodExecutable, MongodProcess> {

    private TokuMXStarter(IRuntimeConfig config) {
        super(config);
    }

    public static TokuMXStarter getInstance(IRuntimeConfig config) {
        return new TokuMXStarter(config);
    }

    public static TokuMXStarter getDefaultInstance() {
        return getInstance(new TokuRuntimeConfigBuilder().defaults(Command.MongoD).build());
    }

    @Override
    protected MongodExecutable newExecutable(IMongodConfig mongodConfig, Distribution distribution, IRuntimeConfig runtime,
            IExtractedFileSet files) {
        return new TokuMongodExecutable(distribution, mongodConfig, runtime, files);
    }
}
