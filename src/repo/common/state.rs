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

use std::collections::{HashMap, HashSet};
use std::fmt::{self, Debug, Formatter};
use std::sync::Mutex;

use cdchunking::ChunkerImpl;
use serde::{Deserialize, Serialize};
use uuid::Uuid;

use crate::store::DataStore;

use super::chunking::IncrementalChunker;
use super::encryption::EncryptionKey;
use super::id_table::UniqueId;
use super::lock::Lock;
use super::metadata::RepoMetadata;
use super::object::Chunk;
use futures::future::BoxFuture;
use std::future::Future;
use std::io;
use std::pin::Pin;
use std::task::Context;

/// Information about a chunk in a repository.
#[derive(Debug, PartialEq, Eq, Clone, Default, Serialize, Deserialize)]
pub struct ChunkInfo {
    /// The ID of the block in the data store which stores this chunk.
    pub block_id: Uuid,

    /// The IDs of objects which reference this chunk.
    pub references: HashSet<UniqueId>,
}

/// The state associated with an `ObjectRepo`.
#[derive(Debug)]
pub struct RepoState<S: DataStore> {
    /// The data store which backs this repository.
    pub store: Mutex<S>,

    /// The metadata for the repository.
    pub metadata: RepoMetadata,

    /// A map of chunk hashes to information about them.
    pub chunks: HashMap<Chunk, ChunkInfo>,

    /// The master encryption key for the repository.
    pub master_key: EncryptionKey,

    /// The lock on the repository.
    pub lock: Lock,
}

/// The location of a chunk in a stream of bytes.
#[derive(Debug, PartialEq, Eq, Clone, Default)]
pub struct ChunkLocation {
    /// The chunk itself.
    pub chunk: Chunk,

    /// The offset of the start of the chunk from the beginning of the object.
    pub start: u64,

    /// The offset of the end of the chunk from the beginning of the object.
    pub end: u64,

    /// The offset of the seek position from the beginning of the object.
    pub position: u64,

    /// The index of the chunk in the list of chunks.
    pub index: usize,
}

impl ChunkLocation {
    /// The offset of the seek position from the beginning of the chunk.
    pub fn relative_position(&self) -> usize {
        (self.position - self.start) as usize
    }
}

/// The current operation being performed by and object.
pub enum ObjectOperation<'a> {
    Read(BoxFuture<'a, io::Result<()>>),
    Write(BoxFuture<'a, io::Result<usize>>),
    Flush(BoxFuture<'a, io::Result<()>>),
    None,
}

/// The state associated with an `Object`.
pub struct ObjectState<'a> {
    /// An object responsible for buffering and chunking data which has been written.
    pub chunker: IncrementalChunker,

    /// The list of chunks which have been written since `flush` was last called.
    pub new_chunks: Vec<Chunk>,

    /// The location of the first chunk written to since `flush` was last called.
    ///
    /// If no data has been written, this is `None`.
    pub start_location: Option<ChunkLocation>,

    /// The next seek position to seek to.
    ///
    /// This value is updated when `AsyncSeek::start_seek` is called, but the actual seek isn't
    /// performed until, `AsyncSeek::poll_complete` is called. Once the seek is performed, this
    /// value is set to `None`. If this value is `Some`, that means `AsyncSeek::start_seek` has been
    /// called but `AsyncSeek::poll_complete` has not been.
    pub next_position: Option<u64>,

    /// The current seek position of the object.
    pub position: u64,

    /// The chunk which was most recently read from.
    ///
    /// If no data has been read, this is `None`.
    pub buffered_chunk: Option<Chunk>,

    /// The contents of the chunk which was most recently read from.
    pub read_buffer: Vec<u8>,

    /// Whether unflushed data has been written to the object.
    pub needs_flushed: bool,

    /// The current operation to poll.
    pub current_operation: ObjectOperation<'a>,
}

impl ObjectState {
    /// Create a new empty state for a repository with a given chunk size.
    pub fn new(chunker: Box<dyn ChunkerImpl>) -> Self {
        Self {
            chunker: IncrementalChunker::new(chunker),
            new_chunks: Vec::new(),
            start_location: None,
            next_position: None,
            position: 0,
            buffered_chunk: None,
            read_buffer: Vec::new(),
            needs_flushed: false,
            current_operation: ObjectOperation::None,
        }
    }
}

impl Debug for ObjectState {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        write!(f, "ObjectState")
    }
}
