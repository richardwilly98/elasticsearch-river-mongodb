package org.elasticsearch.river.mongodb.embed;

import org.apache.commons.lang3.Validate;

import de.flapdoodle.embed.mongo.Command;
import de.flapdoodle.embed.process.extract.UUIDTempNaming;
import de.flapdoodle.embed.process.io.directories.UserHome;
import de.flapdoodle.embed.process.io.progress.StandardConsoleProgressListener;

public class TokuDownloadConfigBuilder extends de.flapdoodle.embed.process.config.store.DownloadConfigBuilder {

    private static final String DEFAULT_DOWNLOAD_PATH = "http://www.tokutek.com/tokumx-for-mongodb/tokumx-community-edition-download/?file=";

    public TokuDownloadConfigBuilder packageResolverForCommand(Command command) {
        Validate.isTrue(command == Command.MongoD, "Only command de.flapdoodle.embed.mongo.Command.MongoD is currently supported");
        packageResolver(new TokuPaths());
        return this;
    }

    public TokuDownloadConfigBuilder defaultsForCommand(Command command) {
        return defaults().packageResolverForCommand(command);
    }

    public TokuDownloadConfigBuilder defaults() {
        fileNaming().setDefault(new UUIDTempNaming());
        downloadPath(DEFAULT_DOWNLOAD_PATH);
        progressListener().setDefault(new StandardConsoleProgressListener());
        artifactStorePath().setDefault(new UserHome(".embedmongo"));
        downloadPrefix().setDefault(new DownloadPrefix("embedtokumx-download"));
        userAgent().setDefault(new UserAgent(
            "Mozilla/5.0 (compatible; elasticsearch-river-mongodb tests; https://github.com/richardwilly98/elasticsearch-river-mongodb)"));
        return this;
    }
}
