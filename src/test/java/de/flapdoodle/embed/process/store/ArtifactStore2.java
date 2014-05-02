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

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.EnumSet;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.logging.Logger;
import java.util.regex.Pattern;

import de.flapdoodle.embed.process.config.store.FileType;
import de.flapdoodle.embed.process.config.store.IDownloadConfig;
import de.flapdoodle.embed.process.config.store.ILibraryStore;
import de.flapdoodle.embed.process.config.store.IPackageResolver;
import de.flapdoodle.embed.process.distribution.Distribution;
import de.flapdoodle.embed.process.distribution.Platform;
import de.flapdoodle.embed.process.extract.Extractors;
import de.flapdoodle.embed.process.extract.FilesToExtract;
import de.flapdoodle.embed.process.extract.IExtractedFileSet;
import de.flapdoodle.embed.process.extract.IExtractor;
import de.flapdoodle.embed.process.extract.ITempNaming;
import de.flapdoodle.embed.process.io.directories.IDirectory;
import de.flapdoodle.embed.process.io.file.Files;


public class ArtifactStore2 implements IArtifactStore {
  private static Logger logger = Logger.getLogger(ArtifactStore.class.getName());

  private IDownloadConfig _downloadConfig;
  private IDirectory _tempDirFactory;
  private ITempNaming _executableNaming;
  
  public ArtifactStore2(IDownloadConfig downloadConfig,IDirectory tempDirFactory,ITempNaming executableNaming) {
    _downloadConfig=downloadConfig;
    _tempDirFactory = tempDirFactory;
    _executableNaming = executableNaming;
  }
  
  @Override
  public boolean checkDistribution(Distribution distribution) throws IOException {
    if (!LocalArtifactStore.checkArtifact(_downloadConfig, distribution)) {
      return LocalArtifactStore.store(_downloadConfig, distribution, Downloader.download(_downloadConfig, distribution));
    }
    return true;
  }

  @Override
  public IExtractedFileSet extractFileSet(Distribution distribution) throws IOException {
    IPackageResolver packageResolver = _downloadConfig.getPackageResolver();
    File artifact = LocalArtifactStore.getArtifact(_downloadConfig, distribution);
    IExtractor extractor = Extractors.getExtractor(packageResolver.getArchiveType(distribution));

    IExtractedFileSet extracted=extractor.extract(_downloadConfig, artifact, new FilesToExtract(_tempDirFactory, _executableNaming, packageResolver.getFileSet(distribution)));
    
    return extracted;
  }

  @Override
  public void removeFileSet(Distribution distribution, IExtractedFileSet all) {
    for (FileType type : EnumSet.complementOf(EnumSet.of(FileType.Executable))) {
      for (File file : all.files(type)) {
        if (file.exists() && !Files.forceDelete(file))
          logger.warning("Could not delete "+type+" NOW: " + file);               
      }
    }
    File exe=all.executable();
    if (exe.exists() && !Files.forceDelete(exe)) {
      logger.warning("Could not delete executable NOW: " + exe);
    }
  }
}
