package de.flapdoodle.embed.process.store;

import java.io.File;
import java.io.IOException;

import de.flapdoodle.embed.process.config.store.IDownloadConfig;
import de.flapdoodle.embed.process.distribution.Distribution;

/**
 * TODO: delete this after upgrading to the next version of de.flapdoodle.embed
 * This has now been added to de.flapdoodle.embed.process
 * https://github.com/flapdoodle-oss/de.flapdoodle.embed.process/commit/24a51a14bd77137f19c635d1a448233316e26892
 */
public interface IDownloader {

    String getDownloadUrl(IDownloadConfig runtime, Distribution distribution);

    File download(IDownloadConfig runtime, Distribution distribution) throws IOException;
}
