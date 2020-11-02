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

use std::collections::HashSet;

use async_trait::async_trait;
use tokio::task::spawn_blocking;
use uuid::Uuid;

use crate::store::DataStore;

use super::id_table::UniqueId;
use super::object::{chunk_hash, Chunk};
use super::state::{ChunkInfo, RepoState};

/// Encode and decode chunks of data.
#[async_trait]
pub trait ChunkEncoder {
    /// Compress and encrypt the given `data` and return it.
    async fn encode_data(&self, data: Vec<u8>) -> crate::Result<Vec<u8>>;

    /// Decrypt and decompress the given `data` and return it.
    async fn decode_data(&self, data: Vec<u8>) -> crate::Result<Vec<u8>>;
}

#[async_trait]
impl<S: DataStore> ChunkEncoder for RepoState<S> {
    async fn encode_data(&self, data: Vec<u8>) -> crate::Result<Vec<u8>> {
        let compression = self.metadata.compression.clone();
        let encryption = self.metadata.encryption.clone();
        let master_key = self.master_key.clone();
        spawn_blocking(move || {
            compression
                .compress(data.as_slice())
                .map(|compressed_data| encryption.encrypt(compressed_data.as_slice(), &master_key))
        })
        .await
        .unwrap()
    }

    async fn decode_data(&self, data: Vec<u8>) -> crate::Result<Vec<u8>> {
        let compression = self.metadata.compression.clone();
        let encryption = self.metadata.encryption.clone();
        let master_key = self.master_key.clone();
        spawn_blocking(move || {
            encryption
                .decrypt(data.as_slice(), &master_key)
                .and_then(|decrypted_data| compression.decompress(decrypted_data.as_slice()))
        })
        .await
        .unwrap()
    }
}

/// Read chunks of data.
#[async_trait]
pub trait ChunkReader {
    /// Return the bytes of the chunk with the given checksum.
    async fn read_chunk(&self, chunk: Chunk) -> crate::Result<Vec<u8>>;
}

#[async_trait]
impl<S: DataStore> ChunkReader for RepoState<S> {
    async fn read_chunk(&self, chunk: Chunk) -> crate::Result<Vec<u8>> {
        let chunk_info = self.chunks.get(&chunk).ok_or(crate::Error::InvalidData)?;
        let chunk = self
            .store
            .lock()
            .unwrap()
            .read_block(chunk_info.block_id)
            .await
            .map_err(|error| crate::Error::Store(anyhow::Error::from(error)))?
            .ok_or(crate::Error::InvalidData)?;

        self.decode_data(chunk).await
    }
}

/// Write chunks of data.
#[async_trait]
pub trait ChunkWriter {
    /// Write the given `data` as a new chunk and returns its checksum.
    ///
    /// If a chunk with the given `data` already exists, its checksum may be returned without
    /// writing any new data.
    ///
    /// This requires a unique `id` which is used for reference counting.
    async fn write_chunk(&mut self, data: Vec<u8>, id: UniqueId) -> crate::Result<Chunk>;
}

#[async_trait]
impl<S: DataStore> ChunkWriter for RepoState<S> {
    async fn write_chunk(&mut self, data: Vec<u8>, id: UniqueId) -> crate::Result<Chunk> {
        // Get a checksum of the unencoded data.
        let chunk = Chunk {
            hash: chunk_hash(data.as_slice()).await,
            size: data.len(),
        };

        // Check if the chunk already exists.
        if let Some(chunk_info) = self.chunks.get_mut(&chunk) {
            chunk_info.references.insert(id);
            return Ok(chunk);
        }

        // Encode the data.
        let encoded_data = self.encode_data(data).await?;
        let block_id = Uuid::new_v4();

        // Write the data to the data store.
        self.store
            .lock()
            .unwrap()
            .write_block(block_id, &encoded_data)
            .await
            .map_err(|error| crate::Error::Store(anyhow::Error::from(error)))?;

        // Add the chunk to the header.
        let chunk_info = ChunkInfo {
            block_id,
            references: {
                let mut id_set = HashSet::new();
                id_set.insert(id);
                id_set
            },
        };
        self.chunks.insert(chunk, chunk_info);

        Ok(chunk)
    }
}
