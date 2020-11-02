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

#![cfg(feature = "store-redis")]

use std::fmt::{self, Debug, Formatter};

use async_trait::async_trait;
use redis::{aio::Connection, AsyncCommands, RedisError};
use uuid::Uuid;

use crate::store::common::DataStore;

/// A UUID which acts as the version ID of the store format.
const CURRENT_VERSION: &str = "b733bd82-4206-11ea-a3dc-7354076bdaf9";

/// A `DataStore` which stores data on a Redis server.
///
/// The `store-redis` cargo feature is required to use this.
pub struct RedisStore {
    connection: Connection,
}

impl Debug for RedisStore {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        write!(f, "RedisStore")
    }
}

impl RedisStore {
    /// Open or create a `RedisStore` using the given `connection`.
    ///
    /// # Errors
    /// - `Error::UnsupportedFormat`: The repository is an unsupported format. This can mean that
    /// this is not a valid `RedisStore` or this repository format is no longer supported by the
    /// library.
    /// - `Error::Store`: An error occurred with the data store.
    /// - `Error::Io`: An I/O error occurred.
    pub async fn new(mut connection: Connection) -> crate::Result<Self> {
        let version_response: Option<String> = connection
            .get("version")
            .await
            .map_err(|error| crate::Error::Store(anyhow::Error::from(error)))?;

        match version_response {
            Some(version) => {
                if version != CURRENT_VERSION {
                    return Err(crate::Error::UnsupportedFormat);
                }
            }
            None => connection
                .set("version", CURRENT_VERSION)
                .await
                .map_err(|error| crate::Error::Store(anyhow::Error::from(error)))?,
        }

        Ok(RedisStore { connection })
    }
}

#[async_trait]
impl DataStore for RedisStore {
    type Error = RedisError;

    async fn write_block(&mut self, id: Uuid, data: &[u8]) -> Result<(), Self::Error> {
        let key_id = id.to_hyphenated().to_string();
        self.connection
            .set(format!("block:{}", key_id), data)
            .await?;
        Ok(())
    }

    async fn read_block(&mut self, id: Uuid) -> Result<Option<Vec<u8>>, Self::Error> {
        let key_id = id.to_hyphenated().to_string();
        self.connection.get(format!("block:{}", key_id)).await
    }

    async fn remove_block(&mut self, id: Uuid) -> Result<(), Self::Error> {
        let key_id = id.to_hyphenated().to_string();
        self.connection.del(format!("block:{}", key_id)).await?;
        Ok(())
    }

    async fn list_blocks(&mut self) -> Result<Vec<Uuid>, Self::Error> {
        let blocks = self
            .connection
            .keys::<_, Vec<String>>("block:*")
            .await?
            .iter()
            .map(|key| {
                let uuid = key.trim_start_matches("block:");
                Uuid::parse_str(uuid).expect("Could not parse UUID.")
            })
            .collect();
        Ok(blocks)
    }
}
