/**
 * Derived from de.flapdoodle.embed.process.store.Downloader
 */
package org.elasticsearch.river.mongodb.embed;

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.HttpCookie;
import java.net.HttpURLConnection;
import java.net.URL;
import java.util.List;
import java.util.logging.Logger;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import de.flapdoodle.embed.process.config.store.IDownloadConfig;
import de.flapdoodle.embed.process.config.store.ITimeoutConfig;
import de.flapdoodle.embed.process.distribution.Distribution;
import de.flapdoodle.embed.process.io.directories.PropertyOrPlatformTempDir;
import de.flapdoodle.embed.process.io.file.Files;
import de.flapdoodle.embed.process.io.progress.IProgressListener;
import de.flapdoodle.embed.process.store.ArtifactStore;
import de.flapdoodle.embed.process.store.IDownloader;

/**
 * Class for downloading runtime
 */
public class TokuDownloader implements IDownloader {

  static final int DEFAULT_CONTENT_LENGTH = 20 * 1024 * 1024;
  static final int BUFFER_LENGTH = 1024 * 8 * 8;
  static final int READ_COUNT_MULTIPLIER = 100;
  private static Logger logger = Logger.getLogger(ArtifactStore.class.getName());
  private static final Pattern REFRESH_HEADER_PATTERN = Pattern.compile("^\\d+; url=(.*)");

  public TokuDownloader() {

  }

  public String getDownloadUrl(IDownloadConfig runtime, Distribution distribution) {
    return runtime.getDownloadPath().getPath(distribution) + runtime.getPackageResolver().getPath(distribution);
  }

  public File download(IDownloadConfig runtime, Distribution distribution) throws IOException {

    String progressLabel = "Download " + distribution;
    IProgressListener progress = runtime.getProgressListener();
    progress.start(progressLabel);

    // First get PHPSESSID cookie from download URL
    ITimeoutConfig timeoutConfig = runtime.getTimeoutConfig();

    URL url = new URL(getDownloadUrl(runtime, distribution));
    logger.fine("Distro URL: " + url);
    HttpURLConnection openConnection = (HttpURLConnection) url.openConnection();
    openConnection.setRequestProperty("User-Agent", runtime.getUserAgent());
    openConnection.setConnectTimeout(timeoutConfig.getConnectionTimeout());
    openConnection.setReadTimeout(runtime.getTimeoutConfig().getReadTimeout());
    openConnection.setInstanceFollowRedirects(false);

    openConnection.getContent(); // we only care about the cookies, not the redirect page contents
    List<HttpCookie> cookies = HttpCookie.parse(openConnection.getHeaderField("Set-Cookie"));
    logger.fine("Cookies fetched from distro URL (before redirects): " + cookies);
    while (("" + openConnection.getResponseCode()).startsWith("30")) {
      url = new URL(url, openConnection.getHeaderField("Location"));
      logger.finest("following redirect to: " + url);
      openConnection = (HttpURLConnection) url.openConnection();
      openConnection.setRequestProperty("User-Agent", runtime.getUserAgent());
      openConnection.setConnectTimeout(timeoutConfig.getConnectionTimeout());
      openConnection.setReadTimeout(runtime.getTimeoutConfig().getReadTimeout());
      openConnection.setInstanceFollowRedirects(false);
      openConnection.getContent();
    }
    logger.finest("Headers returned from distro URL (after following redirects): " + openConnection.getHeaderFields());
    String refresh = openConnection.getHeaderField("Refresh");
    openConnection.getInputStream().close();

    // Then fetch actual file, using the received cookies
    File ret = Files.createTempFile(PropertyOrPlatformTempDir.defaultInstance(),
            runtime.getFileNaming().nameFor(runtime.getDownloadPrefix(), "." + runtime.getPackageResolver().getArchiveType(distribution)));
    logger.fine("Saving distro to " + ret.getAbsolutePath());
    if (ret.canWrite()) {

      BufferedOutputStream bos = new BufferedOutputStream(new FileOutputStream(ret));

      URL url2 = new URL("http://www.tokutek.com/download.php?df=1");
      if (refresh != null && !refresh.isEmpty()) {
        Matcher m = REFRESH_HEADER_PATTERN.matcher(refresh);
        if (m.matches()) {
          url2 = new URL(url, m.group(1));
        }
      }
      logger.fine("Ultimate download URL: " + url2);
      openConnection = (HttpURLConnection) url2.openConnection();
      openConnection.setRequestProperty("User-Agent",runtime.getUserAgent());
      
      openConnection.setConnectTimeout(timeoutConfig.getConnectionTimeout());
      openConnection.setReadTimeout(timeoutConfig.getReadTimeout());
      if (!cookies.isEmpty()) {
        StringBuilder cookie = new StringBuilder();
        for (HttpCookie c : cookies) {
          if (cookie.length() > 0) {
            cookie.append("; ");
          }
          cookie.append(c);
        }
        openConnection.setRequestProperty("Cookie", cookie.toString());
      }

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
