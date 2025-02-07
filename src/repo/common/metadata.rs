/*
 * Copyright 2019-2021 Wren Powell
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

use std::collections::HashMap;

use rmp_serde::from_read;
use serde::{Deserialize, Serialize};

use super::config::RepoConfig;
use super::encryption::{EncryptionKey, KeySalt};
use super::handle::{Chunk, HandleIdTable};
use super::state::{ChunkInfo, InstanceId, InstanceInfo, PackIndex};
use crate::store::{BlockId, BlockKey, DataStore, OpenStore};

/// The repository state which is persisted to the data store on each commit.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Header {
    /// The map of chunks to information about them.
    pub chunks: HashMap<Chunk, ChunkInfo>,

    /// A map of block IDs to their locations in packs.
    pub packs: HashMap<BlockId, Vec<PackIndex>>,

    /// A map of instance IDs to information about each instance.
    pub instances: HashMap<InstanceId, InstanceInfo>,

    /// The table of object handle IDs.
    pub handle_table: HandleIdTable,
}

/// Metadata for a repository.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct RepoMetadata {
    /// The unique ID of this repository.
    pub id: RepoId,

    /// The configuration for the repository.
    pub config: RepoConfig,

    /// The master encryption key encrypted with the user's password.
    pub master_key: Vec<u8>,

    /// The salt used to derive a key from the user's password.
    pub salt: KeySalt,

    /// The ID of the chunk which stores the repository header.
    pub header_id: BlockId,
}

impl RepoMetadata {
    /// Decrypt and return the master encryption key.
    ///
    /// # Errors
    /// - `Error::Password`: The password provided is invalid.
    pub fn decrypt_master_key(&self, password: &[u8]) -> crate::Result<EncryptionKey> {
        let user_key = EncryptionKey::derive(
            password,
            &self.salt,
            self.config.encryption.key_size(),
            self.config.memory_limit,
            self.config.operations_limit,
        );
        Ok(EncryptionKey::new(
            self.config
                .encryption
                .decrypt(&self.master_key, &user_key)
                .map_err(|_| crate::Error::Password)?,
        ))
    }
}

impl RepoMetadata {
    /// Create a `RepoInfo` using the metadata in this struct.
    pub fn to_info(&self) -> RepoInfo {
        RepoInfo {
            id: self.id,
            config: self.config.clone(),
        }
    }
}

/// Return information about the repository in the given `store` without opening it.
pub fn peek_info_store(store: &mut impl DataStore) -> crate::Result<RepoInfo> {
    // Read and deserialize the metadata.
    let serialized_metadata = match store
        .read_block(BlockKey::Super)
        .map_err(crate::Error::Store)?
    {
        Some(data) => data,
        None => return Err(crate::Error::NotFound),
    };
    let metadata: RepoMetadata =
        from_read(serialized_metadata.as_slice()).map_err(|_| crate::Error::Corrupt)?;

    Ok(metadata.to_info())
}

/// Return information about the repository in a data store without opening it.
///
/// This accepts the `config` used to open the data store.
///
/// # Errors
/// - `Error::NotFound`: There is no repository in the data store.
/// - `Error::Corrupt`: The repository is corrupt. This is most likely unrecoverable.
/// - `Error::UnsupportedStore`: The data store is an unsupported format. This can happen if
/// the serialized data format changed or if the storage represented by this value does not
/// contain a valid data store.
/// - `Error::Store`: An error occurred with the data store.
/// - `Error::Io`: An I/O error occurred.
pub fn peek_info(config: &impl OpenStore) -> crate::Result<RepoInfo> {
    // Open the data store.
    let mut store = config.open()?;
    peek_info_store(&mut store)
}

uuid_type! {
    /// A UUID which uniquely identifies a repository.
    ///
    /// This ID is different from the instance ID; this ID is shared between all instances of a
    /// repository.
    RepoId
}

/// Information about a repository.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RepoInfo {
    id: RepoId,
    config: RepoConfig,
}

impl RepoInfo {
    /// The unique ID for this repository.
    pub fn id(&self) -> RepoId {
        self.id
    }

    /// The configuration for this repository.
    pub fn config(&self) -> &RepoConfig {
        &self.config
    }
}

/// Statistics about a repository.
#[derive(Debug, Clone)]
pub struct RepoStats {
    pub(super) apparent_size: u64,
    pub(super) actual_size: u64,
    pub(super) repo_size: u64,
}

impl RepoStats {
    /// The apparent size of the current instance.
    ///
    /// This is the sum of the apparent sizes of all the objects in the current instance of the
    /// repository, which includes any sparse holes in those objects.
    pub fn apparent_size(&self) -> u64 {
        self.apparent_size
    }

    /// The actual size of the current instance.
    ///
    /// This is the actual number of bytes stored in objects in the current instance of the
    /// repository, which may be smaller than the [`apparent_size`] due to sparse holes in objects
    /// and deduplication between objects in the current instance.
    ///
    /// [`apparent_size`]: crate::repo::RepoStats::apparent_size
    pub fn actual_size(&self) -> u64 {
        self.actual_size
    }

    /// The actual size of the repository.
    ///
    /// This is the actual number of bytes stored in objects in all instances of the repository,
    /// which may be smaller than the sum of the [`actual_size`] of each instance due to
    /// deduplication between objects in different instances.
    ///
    /// This value is not necessarily the same as the number of bytes stored in the backing data
    /// store, which may be larger or smaller due to compression, encryption, and packing.
    ///
    /// [`actual_size`]: crate::repo::RepoStats::actual_size
    pub fn repo_size(&self) -> u64 {
        self.repo_size
    }
}
