/*
 * Copyright 2019-2020 Wren Powell
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

#![cfg(feature = "store-sftp")]

use std::fmt::{self, Debug, Formatter};
use std::io::{self, Read, Write};
use std::path::{Path, PathBuf};

use async_ssh2::{self, RenameFlags, Sftp};
use async_trait::async_trait;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use uuid::Uuid;

use super::common::DataStore;

// A UUID which acts as the version ID of the directory store format.
const CURRENT_VERSION: &str = "fc299876-c5ff-11ea-ada1-8b0ec1509cde";

// The names of files in the data store.
const BLOCKS_DIRECTORY: &str = "blocks";
const STAGING_DIRECTORY: &str = "stage";
const VERSION_FILE: &str = "version";

/// A `DataStore` which stores data on an SFTP server.
///
/// The `store-sftp` cargo feature is required to use this.
pub struct SftpStore {
    sftp: Sftp,
    path: PathBuf,
}

impl SftpStore {
    /// Create or open an `SftpStore`.
    ///
    /// This accepts an `sftp` value representing a connection to the server and the `path` of the
    /// store on the server.
    ///
    /// # Errors
    /// - `Error::UnsupportedFormat`: The repository is an unsupported format. This can mean that
    /// this is not a valid `SftpStore` or this repository format is no longer supported by the
    /// library.
    /// - `Error::Store`: An error occurred with the data store.
    /// - `Error::Io`: An I/O error occurred.
    pub fn new(sftp: Sftp, path: PathBuf) -> crate::Result<Self> {
        // Create the directories if they don't exist.
        let directories = &[
            &path,
            &path.join(BLOCKS_DIRECTORY),
            &path.join(STAGING_DIRECTORY),
        ];
        for directory in directories {
            if sftp.stat(&directory).await.is_err() {
                sftp.mkdir(&directory, 0o755)
                    .await
                    .map_err(|error| crate::Error::Store(anyhow::Error::from(error)))?;
            }
        }

        let version_path = path.join(VERSION_FILE);

        if sftp.stat(&version_path).await.is_ok() {
            // Read the version ID file.
            let mut version_file = sftp
                .open(&version_path)
                .await
                .map_err(|error| crate::Error::Store(anyhow::Error::from(error)))?;
            let mut version_id = String::new();
            version_file.read_to_string(&mut version_id).await?;

            // Verify the version ID.
            if version_id != CURRENT_VERSION {
                return Err(crate::Error::UnsupportedFormat);
            }
        } else {
            // Write the version ID file.
            let mut version_file = sftp
                .create(&version_path)
                .await
                .map_err(|error| crate::Error::Store(anyhow::Error::from(error)))?;
            version_file.write_all(CURRENT_VERSION.as_bytes()).await?;
        }

        Ok(SftpStore { sftp, path })
    }

    /// Return the path where a block with the given `id` will be stored.
    fn block_path(&self, id: Uuid) -> PathBuf {
        let mut buffer = Uuid::encode_buffer();
        let hex = id.to_simple().encode_lower(&mut buffer);
        self.path.join(BLOCKS_DIRECTORY).join(&hex[..2]).join(hex)
    }

    /// Return the path where a block with the given `id` will be staged.
    fn staging_path(&self, id: Uuid) -> PathBuf {
        let mut buffer = Uuid::encode_buffer();
        let hex = id.to_simple().encode_lower(&mut buffer);
        self.path.join(STAGING_DIRECTORY).join(hex)
    }

    /// Return whether the given remote `path` exists.
    async fn exists(&self, path: &Path) -> bool {
        self.sftp.stat(path).await.is_ok()
    }
}

#[async_trait]
impl DataStore for SftpStore {
    type Error = async_ssh2::Error;

    async fn write_block(&mut self, id: Uuid, data: &[u8]) -> Result<(), Self::Error> {
        let staging_path = self.staging_path(id);
        let block_path = self.block_path(id);

        // If this is the first block its sub-directory, the directory needs to be created.
        let parent = block_path.parent().unwrap();
        if !self.exists(&parent).await {
            self.sftp.mkdir(&parent, 0o755).await?;
        }

        // Write to a staging file and then atomically move it to its final destination.
        let mut staging_file = self.sftp.create(&staging_path).await?;
        staging_file.write_all(data).await?;
        self.sftp
            .rename(
                &staging_path,
                &block_path,
                Some(RenameFlags::ATOMIC | RenameFlags::OVERWRITE),
            )
            .await?;

        // Remove any unused staging files.
        for (path, _) in self
            .sftp
            .readdir(&self.path.join(STAGING_DIRECTORY))
            .await?
        {
            self.sftp.unlink(&path).await?;
        }

        Ok(())
    }

    async fn read_block(&mut self, id: Uuid) -> Result<Option<Vec<u8>>, Self::Error> {
        let block_path = self.block_path(id);

        if !self.exists(&block_path).await {
            return Ok(None);
        }

        let mut file = self.sftp.open(&block_path).await?;

        let mut buffer = Vec::with_capacity(file.stat().await?.size.unwrap_or(0) as usize);
        file.read_to_end(&mut buffer).await?;
        Ok(Some(buffer))
    }

    async fn remove_block(&mut self, id: Uuid) -> Result<(), Self::Error> {
        let block_path = self.block_path(id);

        if !self.exists(&block_path).await {
            return Ok(());
        }

        self.sftp.unlink(&block_path).await?;

        Ok(())
    }

    async fn list_blocks(&mut self) -> Result<Vec<Uuid>, Self::Error> {
        let block_directories = self.sftp.readdir(&self.path.join(BLOCKS_DIRECTORY)).await?;
        let mut block_ids = Vec::new();

        for (block_directory, _) in block_directories {
            for (block_path, _) in self.sftp.readdir(&block_directory).await? {
                let file_name = block_path
                    .file_name()
                    .unwrap()
                    .to_str()
                    .expect("Block file name is invalid.");
                let id = Uuid::parse_str(file_name).expect("Block file name is invalid.");
                block_ids.push(id);
            }
        }

        Ok(block_ids)
    }
}

impl Debug for SftpStore {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        write!(f, "SftpStore {{ path: {:?} }}", self.path)
    }
}
