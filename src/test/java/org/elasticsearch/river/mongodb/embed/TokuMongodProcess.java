package org.elasticsearch.river.mongodb.embed;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.logging.Logger;

import de.flapdoodle.embed.mongo.MongodExecutable;
import de.flapdoodle.embed.mongo.MongodProcess;
import de.flapdoodle.embed.mongo.config.IMongodConfig;
import de.flapdoodle.embed.process.config.IRuntimeConfig;
import de.flapdoodle.embed.process.config.store.FileType;
import de.flapdoodle.embed.process.distribution.Distribution;
import de.flapdoodle.embed.process.distribution.Platform;
import de.flapdoodle.embed.process.extract.IExtractedFileSet;

public class TokuMongodProcess extends MongodProcess {
    private static Logger logger = Logger.getLogger(TokuMongodProcess.class.getName());

    public TokuMongodProcess(Distribution distribution, IMongodConfig config, IRuntimeConfig runtimeConfig,
            MongodExecutable mongodExecutable) throws IOException {
        super(distribution, config, runtimeConfig, mongodExecutable);
    }

    @Override
    protected Map<String, String> getEnvironment(Distribution distribution, IMongodConfig config, IExtractedFileSet exe) {
        logger.info("ExtractedFileSet: " + exe);
        for (FileType type : FileType.values()) {
            logger.info("" + type + " files: " + exe.files(type));
        }
        HashMap<String, String> environment = new HashMap<String, String>();
        // set LD_LIBRARY_PATH
        if (distribution.getPlatform() == Platform.Linux) {
            environment.put("LD_LIBRARY_PATH", exe.executable().getParent());
        }
        return environment;
    }
}
