// Copyright Rivtower Technologies LLC.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

use r2d2::Pool;
use r2d2_sqlite::SqliteConnectionManager;
use rusqlite::types::ToSql;
use rusqlite::NO_PARAMS;
use rusqlite::{Error, Result};
use std::vec::Vec;

pub struct DB {
    pool: Pool<SqliteConnectionManager>,
}

impl DB {
    pub fn new(db_path: &str) -> Self {
        let manager = SqliteConnectionManager::file(db_path);
        let pool = Pool::new(manager).unwrap();

        let conn = pool.get().unwrap();
        // table global store CurrentHash/CurrentHeight/CurrentProof
        let _ = conn.execute(
            "create table if not exists global (
             id integer primary key,
             content BLOB
         )",
            NO_PARAMS,
        );

        // table transactions store tx_hash:tx
        let _ = conn.execute(
            "create table if not exists transactions (
             tx_hash binary(32) primary key,
             tx BLOB
         )",
            NO_PARAMS,
        );

        // table headers store block_height:block_header
        let _ = conn.execute(
            "create table if not exists headers (
             block_height integer primary key,
             block_header BLOB
         )",
            NO_PARAMS,
        );

        // table bodies store block_height:block_body(group of tx_hash)
        let _ = conn.execute(
            "create table if not exists bodies (
             block_height integer primary key,
             block_body BLOB
         )",
            NO_PARAMS,
        );

        // table blockhash store block_height:block_hash
        let _ = conn.execute(
            "create table if not exists blockhash (
             block_height integer primary key,
             block_hash binary(32)
         )",
            NO_PARAMS,
        );

        DB { pool }
    }

    pub fn store(&self, region: u32, key: Vec<u8>, value: Vec<u8>) -> Result<(), String> {
        let conn = self.pool.get().unwrap();
        let ret = match region {
            0 => {
                if key.len() != 8 {
                    return Err("len of key is not correct".to_owned());
                }
                let mut bytes: [u8; 8] = [0; 8];
                bytes[..8].clone_from_slice(&key[..8]);
                let id = i64::from_be_bytes(bytes);
                conn.execute(
                    "INSERT OR REPLACE INTO global (id, content) values (?1, ?2)",
                    &[&id as &dyn ToSql, &value as &dyn ToSql],
                )
            }
            1 => {
                // when mutlti crypt len of hash should be 33
                if key.len() != 32 && key.len() != 33 {
                    return Err("len of key is not correct".to_owned());
                }
                conn.execute(
                    "INSERT OR REPLACE INTO transactions (tx_hash, tx) values (?1, ?2)",
                    &[&key as &dyn ToSql, &value as &dyn ToSql],
                )
            }
            2 => {
                if key.len() != 8 {
                    return Err("len of key is not correct".to_owned());
                }
                let mut bytes: [u8; 8] = [0; 8];
                bytes[..8].clone_from_slice(&key[..8]);
                let block_height = i64::from_be_bytes(bytes);
                conn.execute(
                    "INSERT OR REPLACE INTO headers (block_height, block_header) values (?1, ?2)",
                    &[&block_height as &dyn ToSql, &value as &dyn ToSql],
                )
            }
            3 => {
                if key.len() != 8 {
                    return Err("len of key is not correct".to_owned());
                }
                let mut bytes: [u8; 8] = [0; 8];
                bytes[..8].clone_from_slice(&key[..8]);
                let block_height = i64::from_be_bytes(bytes);
                conn.execute(
                    "INSERT OR REPLACE INTO bodies (block_height, block_body) values (?1, ?2)",
                    &[&block_height as &dyn ToSql, &value as &dyn ToSql],
                )
            }
            4 => {
                if key.len() != 8 {
                    return Err("len of key is not correct".to_owned());
                }
                // when mutlti crypt len of hash should be 33
                if value.len() != 32 && value.len() != 33 {
                    return Err("len of value is not correct".to_owned());
                }
                let mut bytes: [u8; 8] = [0; 8];
                bytes[..8].clone_from_slice(&key[..8]);
                let block_height = i64::from_be_bytes(bytes);
                conn.execute(
                    "INSERT OR REPLACE INTO blockhash (block_height, block_hash) values (?1, ?2)",
                    &[&block_height as &dyn ToSql, &value as &dyn ToSql],
                )
            }
            _ => return Err("id is not correct".to_owned()),
        };
        ret.map(|_| ()).map_err(|e| format!("store error: {:?}", e))
    }

    pub fn load(&self, region: u32, key: Vec<u8>) -> Result<Vec<u8>, String> {
        let conn = self.pool.get().unwrap();
        let ret = match region {
            0 => {
                if key.len() != 8 {
                    return Err("len of key is not correct".to_owned());
                }
                let mut bytes: [u8; 8] = [0; 8];
                bytes[..8].clone_from_slice(&key[..8]);
                let id = i64::from_be_bytes(bytes);
                conn.query_row("SELECT content FROM global WHERE id=?", &[&id], |row| {
                    row.get(0)
                })
            }
            1 => {
                // when mutlti crypt len of hash should be 33
                if key.len() != 32 && key.len() != 33 {
                    return Err("len of key is not correct".to_owned());
                }
                conn.query_row(
                    "SELECT tx FROM transactions WHERE tx_hash=?",
                    &[&key],
                    |row| row.get(0),
                )
            }
            2 => {
                if key.len() != 8 {
                    return Err("len of key is not correct".to_owned());
                }
                let mut bytes: [u8; 8] = [0; 8];
                bytes[..8].clone_from_slice(&key[..8]);
                let block_height = i64::from_be_bytes(bytes);
                conn.query_row(
                    "SELECT block_header FROM headers WHERE block_height=?",
                    &[&block_height],
                    |row| row.get(0),
                )
            }
            3 => {
                if key.len() != 8 {
                    return Err("len of key is not correct".to_owned());
                }
                let mut bytes: [u8; 8] = [0; 8];
                bytes[..8].clone_from_slice(&key[..8]);
                let block_height = i64::from_be_bytes(bytes);
                conn.query_row(
                    "SELECT block_body FROM bodies WHERE block_height=?",
                    &[&block_height],
                    |row| row.get(0),
                )
            }
            4 => {
                if key.len() != 8 {
                    return Err("len of key is not correct".to_owned());
                }
                let mut bytes: [u8; 8] = [0; 8];
                bytes[..8].clone_from_slice(&key[..8]);
                let block_height = i64::from_be_bytes(bytes);
                conn.query_row(
                    "SELECT block_hash FROM blockhash WHERE block_height=?",
                    &[&block_height],
                    |row| row.get(0),
                )
            }
            _ => return Err("id is not correct".to_owned()),
        };
        if ret == Err(Error::QueryReturnedNoRows) {
            Ok(vec![])
        } else if ret.is_err() {
            Err(format!("load error: {:?}", ret))
        } else {
            Ok(ret.unwrap())
        }
    }

    pub fn delete(&self, region: u32, key: Vec<u8>) -> Result<(), String> {
        let conn = self.pool.get().unwrap();
        let ret = match region {
            0 => {
                if key.len() != 8 {
                    return Err("len of key is not correct".to_owned());
                }
                let mut bytes: [u8; 8] = [0; 8];
                bytes[..8].clone_from_slice(&key[..8]);
                let id = i64::from_be_bytes(bytes);
                conn.execute("DELETE FROM global WHERE id=?", &[&id])
            }
            1 => {
                // when mutlti crypt len of hash should be 33
                if key.len() != 32 && key.len() != 33 {
                    return Err("len of key is not correct".to_owned());
                }
                conn.execute("DELETE FROM transactions WHERE tx_hash=?", &[&key])
            }
            2 => {
                if key.len() != 8 {
                    return Err("len of key is not correct".to_owned());
                }
                let mut bytes: [u8; 8] = [0; 8];
                bytes[..8].clone_from_slice(&key[..8]);
                let block_height = i64::from_be_bytes(bytes);
                conn.execute("DELETE FROM headers WHERE block_height=?", &[&block_height])
            }
            3 => {
                if key.len() != 8 {
                    return Err("len of key is not correct".to_owned());
                }
                let mut bytes: [u8; 8] = [0; 8];
                bytes[..8].clone_from_slice(&key[..8]);
                let block_height = i64::from_be_bytes(bytes);
                conn.execute("DELETE FROM bodies WHERE block_height=?", &[&block_height])
            }
            4 => {
                if key.len() != 8 {
                    return Err("len of key is not correct".to_owned());
                }
                let mut bytes: [u8; 8] = [0; 8];
                bytes[..8].clone_from_slice(&key[..8]);
                let block_height = i64::from_be_bytes(bytes);
                conn.execute(
                    "DELETE FROM blockhash WHERE block_height=?",
                    &[&block_height],
                )
            }
            _ => return Err("id is not correct".to_owned()),
        };
        ret.map(|_| ())
            .map_err(|e| format!("delete error: {:?}", e))
    }
}
