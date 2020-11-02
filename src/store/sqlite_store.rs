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

#![cfg(feature = "store-sqlite")]

use async_trait::async_trait;
use hex_literal::hex;
use sqlx::prelude::*;
use sqlx::{Cursor, Executor, Row, SqliteConnection};
use uuid::Uuid;

use crate::store::common::DataStore;

/// A UUID which acts as the version ID of the store format.
const CURRENT_VERSION: Uuid = Uuid::from_bytes(hex!("08d14eb8 4156 11ea 8ec7 a31cc3dfe2e4"));

/// A `DataStore` which stores data in a SQLite database.
///
/// The `store-sqlite` cargo feature is required to use this.
#[derive(Debug)]
pub struct SqliteStore {
    /// The connection to the SQLite database.
    connection: SqliteConnection,
}

impl SqliteStore {
    /// Open or create a `SqliteStore` with the given database `connection`.
    ///
    /// # Errors
    /// - `Error::UnsupportedFormat`: The repository is an unsupported format. This can mean that
    /// this is not a valid `SqliteStore` or this repository format is no longer supported by the
    /// library.
    /// - `Error::Store`: An error occurred with the data store.
    /// - `Error::Io`: An I/O error occurred.
    pub async fn new(mut connection: SqliteConnection) -> crate::Result<Self> {
        let query = r#"
            CREATE TABLE IF NOT EXISTS Blocks (
                uuid BLOB PRIMARY KEY,
                data BLOB NOT NULL
            );

            CREATE TABLE IF NOT EXISTS Metadata (
                key TEXT PRIMARY KEY,
                value BLOB NOT NULL
            );
        "#;

        connection
            .execute(query)
            .await
            .map_err(|error| crate::Error::Store(anyhow::Error::from(error)))?;

        let query = r#"
            SELECT value FROM Metadata
            WHERE key = 'version';
        "#;

        let version_bytes: Option<Vec<u8>> = connection
            .fetch(query)
            .next()
            .await
            .map_err(|error| crate::Error::Store(anyhow::Error::from(error)))?
            .map(|row| row.get("value"));

        match version_bytes {
            Some(bytes) => {
                let version = Uuid::from_slice(bytes.as_slice())
                    .map_err(|_| crate::Error::UnsupportedFormat)?;
                if version != CURRENT_VERSION {
                    return Err(crate::Error::UnsupportedFormat);
                }
            }
            None => {
                let sql = sqlx::query(
                    r#"
                        INSERT INTO Metadata (key, value)
                        VALUES ('version', ?);
                    "#,
                )
                .bind(&CURRENT_VERSION.as_bytes()[..]);

                connection
                    .execute(sql)
                    .await
                    .map_err(|error| crate::Error::Store(anyhow::Error::from(error)))?;
            }
        }

        Ok(SqliteStore { connection })
    }
}

#[async_trait]
impl DataStore for SqliteStore {
    type Error = sqlx::Error;

    async fn write_block(&mut self, id: Uuid, data: &[u8]) -> Result<(), Self::Error> {
        let sql = sqlx::query(
            r#"
                REPLACE INTO Blocks (uuid, data)
                VALUES (?, ?);
            "#,
        )
        .bind(&id.as_bytes()[..])
        .bind(data);
        self.connection.execute(sql).await?;

        Ok(())
    }

    async fn read_block(&mut self, id: Uuid) -> Result<Option<Vec<u8>>, Self::Error> {
        let sql = sqlx::query(
            r#"
                SELECT data FROM Blocks
                WHERE uuid = ?;
            "#,
        )
        .bind(&id.as_bytes()[..]);

        let data = self
            .connection
            .fetch(sql)
            .next()
            .await?
            .map(|row| row.get("data"));

        Ok(data)
    }

    async fn remove_block(&mut self, id: Uuid) -> Result<(), Self::Error> {
        let sql = sqlx::query(
            r#"
                DELETE FROM Blocks
                WHERE uuid = ?;
            "#,
        )
        .bind(&id.as_bytes()[..]);

        self.connection.execute(sql).await?;

        Ok(())
    }

    async fn list_blocks(&mut self) -> Result<Vec<Uuid>, Self::Error> {
        let query = r#"SELECT uuid FROM Blocks;"#;
        let mut cursor = self.connection.fetch(query);

        let mut result = Vec::new();

        while let Some(row) = cursor.next().await? {
            let id_bytes: Vec<u8> = row.get("uuid");
            let id = Uuid::from_slice(id_bytes.as_slice()).expect("Could not parse UUID.");
            result.push(id);
        }

        Ok(result)
    }
}
