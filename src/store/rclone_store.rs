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

#![cfg(feature = "store-rclone")]

use std::io;
use std::net::{Ipv4Addr, SocketAddr, SocketAddrV4, TcpStream, UdpSocket};
use std::path::Path;
use std::process::{Child, Command, Stdio};
use std::thread::sleep;
use std::time::Duration;

use rand::distributions::Alphanumeric;
use rand::Rng;

use super::data_store::{BlockId, BlockKey, BlockType, DataStore};
use super::open_store::OpenStore;
use super::sftp_store::{SftpAuth, SftpConfig, SftpStore};

/// Generate a random secure password for the SFTP server.
fn generate_password(length: u32) -> String {
    rand::thread_rng()
        .sample_iter(&Alphanumeric)
        .take(length as usize)
        .collect()
}

/// Return an unused ephemeral port number.
fn ephemeral_port() -> io::Result<u16> {
    match UdpSocket::bind("localhost:0")?.local_addr()? {
        SocketAddr::V4(address) => Ok(address.port()),
        SocketAddr::V6(address) => Ok(address.port()),
    }
}

/// The length of the password for the SFTP server.
const PASSWORD_LENGTH: u32 = 30;

/// The username for authenticating the SSH connection.
const SSH_USERNAME: &str = "rclone";

/// The amount of time to wait between attempts to connect to the SFTP server.
const CONNECT_WAIT_TIME: Duration = Duration::from_millis(100);

/// Serve the rclone remote over SFTP and return the server process.
fn serve(port: u16, password: &str, config: &str) -> io::Result<Child> {
    Command::new("rclone")
        .args(&[
            "serve",
            "sftp",
            "--vfs-cache-mode",
            "writes",
            "--addr",
            &format!("localhost:{}", port),
            "--user",
            SSH_USERNAME,
            "--pass",
            password,
            config,
        ])
        .stdin(Stdio::null())
        .stdout(Stdio::null())
        .stderr(Stdio::inherit())
        .spawn()
}

/// Wait for a local TCP connection on the given `port` to connect and then drop the connection.
fn wait_for_connection(port: u16) -> io::Result<()> {
    loop {
        match TcpStream::connect(SocketAddrV4::new(Ipv4Addr::LOCALHOST, port)) {
            Err(error) if error.kind() == io::ErrorKind::ConnectionRefused => {
                sleep(CONNECT_WAIT_TIME);
                continue;
            }
            Err(error) => return Err(error),
            Ok(_) => break,
        }
    }

    Ok(())
}

/// The configuration for opening an [`RcloneStore`].
///
/// [`RcloneStore`]: crate::store::RcloneStore
#[derive(Debug, PartialEq, Eq, Clone)]
#[cfg_attr(docsrs, doc(cfg(feature = "store-rclone")))]
pub struct RcloneConfig {
    /// The rclone remote and path.
    ///
    /// This is a string with the format `<remote>:<path>`, where `<remote>` is the name of the
    /// remote as configured using `rclone config` and `<path>` is the path of the directory on the
    /// remote to use.
    pub config: String,
}

impl OpenStore for RcloneConfig {
    type Store = RcloneStore;

    fn open(&self) -> crate::Result<Self::Store> {
        // Serve the rclone remote over SFTP and wait for the server to start.
        let port = ephemeral_port()?;
        let password = generate_password(PASSWORD_LENGTH);
        let server_process = serve(port, &password, &self.config)?;
        wait_for_connection(port)?;

        let sftp_config = SftpConfig {
            addr: SocketAddrV4::new(Ipv4Addr::LOCALHOST, port).into(),
            auth: SftpAuth::Password {
                username: SSH_USERNAME.to_string(),
                password,
            },
            path: Path::new("").to_owned(),
        };

        let sftp_store = sftp_config.open()?;

        Ok(RcloneStore {
            sftp_store,
            server_process,
        })
    }
}

/// A `DataStore` which stores data in cloud storage using rclone.
///
/// This is a data store which is backed by [rclone](https://rclone.org/), allowing access to a wide
/// variety of cloud storage providers.
///
/// To use this data store, rclone must be installed and available on the `PATH`. Rclone version
/// 1.48.0 or higher is required.
///
/// You can use [`RcloneConfig`] to open a data store of this type.
///
/// [`RcloneConfig`]: crate::store::RcloneConfig
#[derive(Debug)]
#[cfg_attr(docsrs, doc(cfg(feature = "store-rclone")))]
pub struct RcloneStore {
    sftp_store: SftpStore,
    server_process: Child,
}

impl DataStore for RcloneStore {
    fn write_block(&mut self, key: BlockKey, data: &[u8]) -> anyhow::Result<()> {
        self.sftp_store.write_block(key, data)
    }

    fn read_block(&mut self, key: BlockKey) -> anyhow::Result<Option<Vec<u8>>> {
        self.sftp_store.read_block(key)
    }

    fn remove_block(&mut self, key: BlockKey) -> anyhow::Result<()> {
        self.sftp_store.remove_block(key)
    }

    fn list_blocks(&mut self, kind: BlockType) -> anyhow::Result<Vec<BlockId>> {
        self.sftp_store.list_blocks(kind)
    }
}

impl Drop for RcloneStore {
    fn drop(&mut self) {
        self.server_process.kill().ok();
    }
}
