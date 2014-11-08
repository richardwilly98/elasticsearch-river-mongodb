package de.flapdoodle.embed.process.extract;

import java.io.File;
import java.io.IOException;

import de.flapdoodle.embed.process.config.store.FileSet;
import de.flapdoodle.embed.process.io.directories.IDirectory;
import de.flapdoodle.embed.process.io.file.Files;

/**
 * Put all extracted files in a new unique temp dir, to ensure there are no collisions between extractions.
 * The original FilesToExtract tries to write libraries to fixed paths.
 * 
 * TODO: See if we can delete this class after our fix for CachingArtifactStore is committed and we
 * upgrade to a version of the library containing the fix
 * https://github.com/flapdoodle-oss/de.flapdoodle.embed.mongo/pull/104
 * "mvn test" fails without this class at the moment
 */
public class FilesToExtract2 extends FilesToExtract {

    public FilesToExtract2(IDirectory dirFactory, ITempNaming executableNaming, FileSet fileSet) throws IOException {
        this(executableNaming, fileSet, Files.createTempDir(dirFactory, "extract"));
    }

    private FilesToExtract2(ITempNaming executableNaming, FileSet fileSet, final File subdir) {
        this(executableNaming, fileSet, new IDirectory() {
            @Override public File asFile() { return subdir; }
            @Override public boolean isGenerated() { return true; }
        });
    }

    private FilesToExtract2(ITempNaming executableNaming, FileSet fileSet, IDirectory subdirFactory) {
        super(subdirFactory, executableNaming, fileSet);
    }

}
