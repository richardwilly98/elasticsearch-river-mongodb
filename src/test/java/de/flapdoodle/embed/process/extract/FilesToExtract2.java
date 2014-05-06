/**
 * Put all extracted files in a new unique temp dir, to ensure there are no collisions between extractions.
 * The original FilesToExtract tries to write libraries to fixed paths.
 */
package de.flapdoodle.embed.process.extract;

import java.io.File;
import java.io.IOException;

import de.flapdoodle.embed.process.config.store.FileSet;
import de.flapdoodle.embed.process.io.directories.IDirectory;
import de.flapdoodle.embed.process.io.file.Files;

public class FilesToExtract2 extends FilesToExtract {

    public FilesToExtract2(IDirectory dirFactory, ITempNaming exeutableNaming, FileSet fileSet) throws IOException {
        this(exeutableNaming, fileSet, Files.createTempDir(dirFactory, "extract"));
    }

    private FilesToExtract2(ITempNaming exeutableNaming, FileSet fileSet, final File subdir) {
        this(exeutableNaming, fileSet, new IDirectory() { @Override public File asFile() { return subdir; }});
    }

    private FilesToExtract2(ITempNaming exeutableNaming, FileSet fileSet, IDirectory subdirFactory) {
        super(subdirFactory, exeutableNaming, fileSet);
    }

}
