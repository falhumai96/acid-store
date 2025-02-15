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

use std::cmp::min;
use std::io::{self, Read};
use std::ops::Range;

use serde::{Deserialize, Serialize};

use super::metadata::RepoId;

id_table! {
    /// An ID which uniquely identifies an object in a repository instance.
    HandleId

    /// A table for allocating `HandleId` values.
    HandleIdTable
}

/// A checksum used for uniquely identifying a chunk.
pub type ChunkHash = [u8; blake3::OUT_LEN];

/// Compute the BLAKE3 checksum of the given `data` and return the result.
pub fn chunk_hash(data: &[u8]) -> ChunkHash {
    blake3::hash(data).into()
}

/// A chunk of data generated by the chunking algorithm.
#[derive(Debug, PartialEq, Eq, Hash, Clone, Copy, Serialize, Deserialize)]
pub struct Chunk {
    /// The size of the chunk in bytes.
    pub size: u32,

    /// The checksum of the chunk.
    pub hash: ChunkHash,
}

/// A contiguous region in an object.
///
/// An object can be represented as a list of extents. An extent can be either a `Chunk`, which is
/// a chunk of data as generated by the chunking algorithm, or a `Hole`, which is a region of empty
/// space in an object that contains no data. The existence of holes allows us to implement sparse
/// objects.
#[derive(Debug, PartialEq, Eq, Hash, Clone, Copy, Serialize, Deserialize)]
pub enum Extent {
    /// A chunk of data.
    Chunk(Chunk),

    /// A region of empty space.
    Hole {
        /// The size of the hole in bytes.
        size: u64,
    },
}

impl Extent {
    /// The size of the extent in bytes.
    pub fn size(&self) -> u64 {
        match self {
            Extent::Chunk(chunk) => chunk.size as u64,
            Extent::Hole { size } => *size,
        }
    }
}

/// A handle for accessing data in a repository.
///
/// An `ObjectHandle` is like an address for locating data stored in a `KeyRepo`.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ObjectHandle {
    /// The ID of this handle which is unique within its repository.
    ///
    /// Handle IDs are only guaranteed to be unique within the same repository.
    pub id: HandleId,

    /// The extents which make up the object.
    pub extents: Vec<Extent>,
}

impl ObjectHandle {
    /// The apparent size of the object in bytes.
    pub fn size(&self) -> u64 {
        self.extents.iter().map(|extent| extent.size()).sum()
    }

    /// Return an iterator over the chunks in this object in order.
    pub fn chunks(&self) -> impl Iterator<Item = Chunk> + '_ {
        self.extents.iter().filter_map(|extent| match extent {
            Extent::Chunk(chunk) => Some(*chunk),
            Extent::Hole { .. } => None,
        })
    }
}

/// A value that represents the identity of an object.
///
/// This value can be used to determine if two [`Object`] or [`ReadOnlyObject`] instances refer to
/// the same underlying object in the repository. This is different from a [`ContentId`], which is
/// used to compare the contents of objects.
///
/// # Examples
/// ```
/// # use acid_store::repo::{OpenOptions, OpenMode};
/// # use acid_store::store::MemoryConfig;
/// # use acid_store::repo::key::KeyRepo;
/// let mut repo: KeyRepo<String> = OpenOptions::new()
///    .mode(OpenMode::CreateNew)
///    .open(&MemoryConfig::new())
///    .unwrap();
///
/// let apple1 = repo.insert(String::from("Apple"));
/// let apple2 = repo.object("Apple").unwrap();
/// let orange = repo.insert(String::from("Orange"));
///
/// assert_eq!(apple1.object_id().unwrap(), apple2.object_id().unwrap());
/// assert_ne!(apple1.object_id().unwrap(), orange.object_id().unwrap());
/// assert_ne!(apple2.object_id().unwrap(), orange.object_id().unwrap());
/// ```
///
/// [`Object`]: crate::repo::Object
/// [`ReadOnlyObject`]: crate::repo::ReadOnlyObject
/// [`ContentId`]: crate::repo::ContentId
#[derive(Debug, PartialEq, Eq, Hash, Clone, Copy)]
pub struct ObjectId {
    // We need to store the repository ID because object handle IDs are only unique within the same
    // repository. However, they are unique among all instances of a repository, so we do not need
    // to store the instance ID.
    repo_id: RepoId,
    handle_id: HandleId,
}

impl ObjectId {
    pub(super) fn new(repo_id: RepoId, handle_id: HandleId) -> Self {
        Self { repo_id, handle_id }
    }
}

/// A value that uniquely identifies the contents of an object at a certain point in time.
///
/// A `ContentId` is like a checksum of the data in an object except it is cheap to compute.
/// A `ContentId` can be compared with other `ContentId` values to determine if the contents of two
/// objects are equal. However, these comparisons are only valid within the same repository;
/// content IDs from different repositories are never equal. To compare data between repositories,
/// you should use [`compare_contents`].
///
/// For the purpose of calculating a content ID, a sparse hole in an object is not equal to a range
/// of null bytes. Writing null bytes to an object with `Write` will produce an object with a
/// different content ID than creating a sparse hole with [`Object::set_len`]. To compare the
/// contents of objects without making this distinction, use [`compare_contents`].
///
/// `ContentId` is opaque, but it can be serialized and deserialized. The value of a `ContentId` is
/// stable, meaning that they can be compared across invocations of the library.
///
/// [`compare_contents`]: crate::repo::ContentId::compare_contents
/// [`Object::set_len`]: crate::repo::Object::set_len
#[derive(Debug, PartialEq, Eq, Hash, Clone, Serialize, Deserialize)]
pub struct ContentId {
    // We can't compare content IDs from different repositories because those repositories may have
    // different a chunking configuration. To ensure consistent behavior, we include the
    // repository's UUID to ensure that content IDs from different repositories are never equal.
    /// The ID of the repository the object is associated with.
    pub(super) repo_id: RepoId,

    /// The extents which make up the data.
    pub(super) extents: Vec<Extent>,
}

/// The maximum number of bytes which will be read when comparing contents against a hole.
const HOLE_BUFFER: usize = 4096;

impl ContentId {
    /// The size of the contents represented by this content ID in bytes.
    pub fn size(&self) -> u64 {
        self.extents.iter().map(|extent| extent.size()).sum()
    }

    /// Return whether this content ID has the same contents as `other`.
    ///
    /// This compares the contents of this content ID with `other` without reading any data from the
    /// data store. This is much faster than calculating a checksum of the object, especially if
    /// reading from the data store would be prohibitively slow.
    ///
    /// This method may not need to read `other` in its entirety to determine that the contents are
    /// different.
    ///
    /// Because `other` only implements `Read`, this cannot compare the contents by size. If you
    /// need to compare this content ID with a file or some other source of data with a known size,
    /// you should use [`size`] to query the size of this content ID so you can handle the trivial
    /// case of the contents having different sizes.
    ///
    /// If you need to compare the contents of two objects from the same repository, it's cheaper to
    /// check if their `ContentId` values are equal instead.
    ///
    /// # Errors
    /// - `Error::Io`: An I/O error occurred.
    ///
    /// [`size`]: crate::repo::ContentId::size
    pub fn compare_contents(&self, mut other: impl Read) -> crate::Result<bool> {
        let mut buffer = vec![0u8; HOLE_BUFFER];

        for extent in &self.extents {
            match extent {
                Extent::Chunk(chunk) => {
                    // Grow the buffer so it's large enough.
                    if buffer.len() < chunk.size as usize {
                        buffer.resize(chunk.size as usize, 0u8);
                    }

                    if let Err(error) = other.read_exact(&mut buffer[..chunk.size as usize]) {
                        return if error.kind() == io::ErrorKind::UnexpectedEof {
                            Ok(false)
                        } else {
                            Err(error.into())
                        };
                    }

                    if chunk.hash != chunk_hash(&buffer[..chunk.size as usize]) {
                        return Ok(false);
                    }
                }
                Extent::Hole { size } => {
                    let mut bytes_remaining = *size;
                    let mut max_read_size;
                    let mut bytes_read;

                    while bytes_remaining > 0 {
                        // We put an upper bound on the number of bytes we can read because holes
                        // can be quite large.
                        max_read_size = min(bytes_remaining as usize, HOLE_BUFFER);

                        bytes_read = other.read(&mut buffer[..max_read_size])?;

                        if bytes_read == 0 {
                            return Ok(false);
                        }

                        if buffer[..bytes_read].iter().any(|&byte| byte != 0) {
                            return Ok(false);
                        }

                        bytes_remaining -= bytes_read as u64;
                    }
                }
            }
        }

        // Handle the case where `other` is longer than this object.
        if other.read(&mut buffer)? != 0 {
            return Ok(false);
        }

        Ok(true)
    }
}

/// Statistics about an [`Object`] or [`ReadOnlyObject`].
///
/// [`Object`]: crate::repo::Object
/// [`ReadOnlyObject`]: crate::repo::ReadOnlyObject
#[derive(Debug, Clone)]
pub struct ObjectStats {
    pub(super) apparent_size: u64,
    pub(super) actual_size: u64,
    pub(super) holes: Vec<Range<u64>>,
}

impl ObjectStats {
    /// The object's apparent size.
    ///
    /// This is the number of bytes in the object including any sparse holes created with
    /// [`Object::set_len`]. This is the same value returned by [`Object::size`].
    ///
    /// [`Object::set_len`]: crate::repo::Object::set_len
    /// [`Object::size`]: crate::repo::Object::size
    pub fn apparent_size(&self) -> u64 {
        self.apparent_size
    }

    /// The object's actual size.
    ///
    /// This is the number of bytes in the object not including any sparse holes created with
    /// [`Object::set_len`].
    ///
    /// [`Object::set_len`]: crate::repo::Object::set_len
    pub fn actual_size(&self) -> u64 {
        self.actual_size
    }

    /// The locations of sparse holes in the object.
    ///
    /// This returns a slice of the ranges of bytes which are sparse holes created with
    /// [`Object::set_len`].
    ///
    /// [`Object::set_len`]: crate::repo::Object::set_len
    pub fn holes(&self) -> &[Range<u64>] {
        &self.holes
    }
}
