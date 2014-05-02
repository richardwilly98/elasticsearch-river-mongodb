/**
 * Copyright (C) 2011
 *   Michael Mosmann <michael@mosmann.de>
 *   Martin Jöhren <m.joehren@googlemail.com>
 *
 * with contributions from
 *  konstantin-ba@github,Archimedes Trajano (trajano@github)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *         http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package de.flapdoodle.embed.process.store;

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.URL;
import java.net.URLConnection;

import de.flapdoodle.embed.process.config.store.IDownloadConfig;
import de.flapdoodle.embed.process.config.store.ITimeoutConfig;
import de.flapdoodle.embed.process.distribution.Distribution;
import de.flapdoodle.embed.process.io.file.Files;
import de.flapdoodle.embed.process.io.progress.IProgressListener;

/**
 * Class for downloading runtime
 */
public class Downloader {

  static final int DEFAULT_CONTENT_LENGTH = 20 * 1024 * 1024;
  static final int BUFFER_LENGTH = 1024 * 8 * 8;
  static final int READ_COUNT_MULTIPLIER = 100;

  private Downloader() {

  }

  public static String getDownloadUrl(IDownloadConfig runtime, Distribution distribution) {
    return runtime.getDownloadPath().getPath(distribution) + runtime.getPackageResolver().getPath(distribution);
  }

  public static File download(IDownloadConfig runtime, Distribution distribution) throws IOException {

    String progressLabel = "Download " + distribution;
    IProgressListener progress = runtime.getProgressListener();
    progress.start(progressLabel);

    File ret = Files.createTempFile(runtime.getFileNaming()
        .nameFor(runtime.getDownloadPrefix(), "." + runtime.getPackageResolver().getArchiveType(distribution)));
    if (ret.canWrite()) {

      BufferedOutputStream bos = new BufferedOutputStream(new FileOutputStream(ret));

      URL url = new URL(getDownloadUrl(runtime, distribution));
      URLConnection openConnection = url.openConnection();
      openConnection.setRequestProperty("User-Agent",runtime.getUserAgent());
      
      ITimeoutConfig timeoutConfig = runtime.getTimeoutConfig();
      
      openConnection.setConnectTimeout(timeoutConfig.getConnectionTimeout());
      openConnection.setReadTimeout(runtime.getTimeoutConfig().getReadTimeout());

      InputStream downloadStream = openConnection.getInputStream();

      long length = openConnection.getContentLength();
      progress.info(progressLabel, "DownloadSize: " + length);

      if (length == -1) length = DEFAULT_CONTENT_LENGTH;


      long downloadStartedAt = System.currentTimeMillis();
      
      try {
        BufferedInputStream bis = new BufferedInputStream(downloadStream);
        byte[] buf = new byte[BUFFER_LENGTH];
        int read = 0;
        long readCount = 0;
        while ((read = bis.read(buf)) != -1) {
          bos.write(buf, 0, read);
          readCount = readCount + read;
          if (readCount > length) length = readCount;

          progress.progress(progressLabel, (int) (readCount * READ_COUNT_MULTIPLIER / length));
        }
        progress.info(progressLabel, "downloaded with " + downloadSpeed(downloadStartedAt,length));
      } finally {
        downloadStream.close();
        bos.flush();
        bos.close();
      }
    } else {
      throw new IOException("Can not write " + ret);
    }
    progress.done(progressLabel);
    return ret;
  }

  private static String downloadSpeed(long downloadStartedAt,long downloadSize) {
    long timeUsed=(System.currentTimeMillis()-downloadStartedAt)/1000;
    if (timeUsed==0) {
      timeUsed=1;
    }
    long kbPerSecond=downloadSize/(timeUsed*1024);
    return ""+kbPerSecond+"kb/s";
  }


}
