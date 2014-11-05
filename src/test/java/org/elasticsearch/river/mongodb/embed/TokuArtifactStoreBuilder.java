package org.elasticsearch.river.mongodb.embed;

import de.flapdoodle.embed.mongo.Command;
import de.flapdoodle.embed.process.config.store.ILibraryStore;
import de.flapdoodle.embed.process.config.store.PlatformLibraryStoreBuilder;
import de.flapdoodle.embed.process.distribution.Platform;
import de.flapdoodle.embed.process.extract.UUIDTempNaming;
import de.flapdoodle.embed.process.io.directories.PropertyOrPlatformTempDir;

public class TokuArtifactStoreBuilder extends de.flapdoodle.embed.process.store.ArtifactStore2Builder {

    public TokuArtifactStoreBuilder defaults(Command command) {
        tempDir().setDefault(new PropertyOrPlatformTempDir());
        executableNaming().setDefault(new UUIDTempNaming());
        download().setDefault(new TokuDownloadConfigBuilder().defaultsForCommand(command).build());
        downloader().setDefault(new TokuDownloader());
        libraries().setDefault(libraryStore());
        return this;
    }

    private ILibraryStore libraryStore() {
        PlatformLibraryStoreBuilder libraryStoreBuilder = new PlatformLibraryStoreBuilder().defaults();
        libraryStoreBuilder.setLibraries(Platform.Linux, new String[] { "libHotBackup.so" });
        return libraryStoreBuilder.build();
    }
}
