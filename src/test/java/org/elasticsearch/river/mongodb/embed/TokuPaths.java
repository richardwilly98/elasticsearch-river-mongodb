package org.elasticsearch.river.mongodb.embed;

import de.flapdoodle.embed.mongo.Command;
import de.flapdoodle.embed.mongo.Paths;
import de.flapdoodle.embed.process.config.store.FileSet;
import de.flapdoodle.embed.process.config.store.FileType;
import de.flapdoodle.embed.process.distribution.BitSize;
import de.flapdoodle.embed.process.distribution.Distribution;

public class TokuPaths extends Paths {
    public TokuPaths() {
        super(Command.MongoD);
    }

    @Override
    public FileSet getFileSet(Distribution distribution) {
        String executableFileName;
        switch (distribution.getPlatform()) {
        case Linux:
        case OS_X:
        case Solaris:
            executableFileName = Command.MongoD.commandName();
            break;
        case Windows:
            executableFileName = Command.MongoD.commandName() + ".exe";
            break;
        default:
            throw new IllegalArgumentException("Unknown Platform " + distribution.getPlatform());
        }
        FileSet.Builder builder = FileSet.builder().addEntry(FileType.Executable, executableFileName);
        builder.addEntry(FileType.Library, "libHotBackup.so");
        builder.addEntry(FileType.Library, "libtokufractaltree.so", ".*/lib64/libtokufractaltree\\.so$");
        builder.addEntry(FileType.Library, "libtokuportability.so");
        return builder.build();
    }

    @Override
    public String getPath(Distribution distribution) {
        if (distribution.getBitsize() != BitSize.B64) {
            throw new IllegalArgumentException("Only 64-bit systems are currently supported");
        }

        String splatform;
        switch (distribution.getPlatform()) {
        case Linux:
            splatform = "linux";
            break;
        case Windows:
            splatform = "win32";
            break;
        case OS_X:
            splatform = "osx";
            break;
        case Solaris:
            splatform = "sunos5";
            break;
        default:
            throw new IllegalArgumentException("Unknown Platform " + distribution.getPlatform());
        }

        return "tokumx-" + getVersionPart(distribution.getVersion()) + '-' + splatform + "-x86_64-main.tar.gz";
    }
}
