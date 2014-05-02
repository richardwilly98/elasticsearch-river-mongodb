package de.flapdoodle.embed.process.store;

import java.io.File;
import java.io.IOException;

import de.flapdoodle.embed.process.config.store.IDownloadConfig;
import de.flapdoodle.embed.process.distribution.Distribution;

public interface IDownloader {

    String getDownloadUrl(IDownloadConfig runtime, Distribution distribution);

    File download(IDownloadConfig runtime, Distribution distribution) throws IOException;
}
