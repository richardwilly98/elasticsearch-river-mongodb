package org.elasticsearch.river.mongodb.embed;

import de.flapdoodle.embed.mongo.Command;
import de.flapdoodle.embed.mongo.config.RuntimeConfigBuilder;

public class TokuRuntimeConfigBuilder extends RuntimeConfigBuilder {

    public TokuRuntimeConfigBuilder defaults(Command command) {
        super.defaults(command);
        artifactStore().overwriteDefault(new TokuArtifactStoreBuilder().defaults(command).build());
        return this;
    }
}
