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
use rusqlite::{Error, Result};
use std::vec::Vec;

pub struct DB {
    pool: Pool<SqliteConnectionManager>,
}

impl DB {
    pub fn new(db_path: &str) -> Self {
        let manager = SqliteConnectionManager::file(db_path)
            .with_init(|c| c.execute_batch("PRAGMA synchronous=OFF;"));
        let pool = Pool::new(manager).unwrap();

        let conn = pool.get().unwrap();
        // table 0 global store CurrentHash/CurrentHeight/CurrentProof
        let _ = conn.execute(
            "create table if not exists global (
             id integer primary key,
             content BLOB
         )",
            [],
        );

        // table 1 transactions store tx_hash:tx
        let _ = conn.execute(
            "create table if not exists transactions (
             tx_hash binary(32) primary key,
             tx BLOB
         )",
            [],
        );

        // table 2 headers store block_height:block_header
        let _ = conn.execute(
            "create table if not exists headers (
             block_height integer primary key,
             block_header BLOB
         )",
            [],
        );

        // table 3 bodies store block_height:block_body(group of tx_hash)
        let _ = conn.execute(
            "create table if not exists bodies (
             block_height integer primary key,
             block_body BLOB
         )",
            [],
        );

        // table 4 blockhash store block_height:block_hash
        let _ = conn.execute(
            "create table if not exists blockhash (
             block_height integer primary key,
             block_hash binary(32)
         )",
            [],
        );

        // table 5 proofs store block_height:proof
        let _ = conn.execute(
            "create table if not exists proofs (
             block_height integer primary key,
             proof BLOB
         )",
            [],
        );

        // table 6 results store block_height:executed_block_hash
        let _ = conn.execute(
            "create table if not exists results (
             block_height integer primary key,
             executed_block_hash binary(32)
         )",
            [],
        );

        // table 7 tx_hash_2_block_height store tx_hash:block_height
        let _ = conn.execute(
            "create table if not exists tx_hash_2_block_height (
             tx_hash binary(32) primary key,
             block_height integer
         )",
            [],
        );

        // table 8 results store block_hash:block_height
        // this is a sql db, so we can reuse table 4

        // table 9 transaction_index store tx_hash:tx_index
        let _ = conn.execute(
            "create table if not exists transaction_index (
             tx_hash binary(32) primary key,
             tx_index integer
         )",
            [],
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
                if key.len() != 32 {
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

                if value.len() != 32 {
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
            5 => {
                if key.len() != 8 {
                    return Err("len of key is not correct".to_owned());
                }
                let mut bytes: [u8; 8] = [0; 8];
                bytes[..8].clone_from_slice(&key[..8]);
                let block_height = i64::from_be_bytes(bytes);
                conn.execute(
                    "INSERT OR REPLACE INTO proofs (block_height, proof) values (?1, ?2)",
                    &[&block_height as &dyn ToSql, &value as &dyn ToSql],
                )
            }
            6 => {
                if key.len() != 8 {
                    return Err("len of key is not correct".to_owned());
                }

                if value.len() != 32 {
                    return Err("len of value is not correct".to_owned());
                }
                let mut bytes: [u8; 8] = [0; 8];
                bytes[..8].clone_from_slice(&key[..8]);
                let block_height = i64::from_be_bytes(bytes);
                conn.execute(
                    "INSERT OR REPLACE INTO results (block_height, executed_block_hash) values (?1, ?2)",
                    &[&block_height as &dyn ToSql, &value as &dyn ToSql],
                )
            }
            7 => {
                if key.len() != 32 {
                    return Err("len of key is not correct".to_owned());
                }
                if value.len() != 8 {
                    return Err("len of value is not correct".to_owned());
                }
                let mut bytes: [u8; 8] = [0; 8];
                bytes[..8].clone_from_slice(&value[..8]);
                let block_height = i64::from_be_bytes(bytes);
                conn.execute(
                    "INSERT OR REPLACE INTO tx_hash_2_block_height (tx_hash, block_height) values (?1, ?2)",
                    &[&key as &dyn ToSql, &block_height as &dyn ToSql],
                )
            }
            8 => {
                if value.len() != 8 {
                    return Err("len of value is not correct".to_owned());
                }
                if key.len() != 32 {
                    return Err("len of key is not correct".to_owned());
                }
                let mut bytes: [u8; 8] = [0; 8];
                bytes[..8].clone_from_slice(&value[..8]);
                let block_height = i64::from_be_bytes(bytes);
                conn.execute(
                    "INSERT OR REPLACE INTO blockhash (block_height, block_hash) values (?1, ?2)",
                    &[&block_height as &dyn ToSql, &key as &dyn ToSql],
                )
            }
            9 => {
                if value.len() != 8 {
                    return Err("len of value is not correct".to_owned());
                }
                if key.len() != 32 {
                    return Err("len of key is not correct".to_owned());
                }
                let mut bytes: [u8; 8] = [0; 8];
                bytes[..8].clone_from_slice(&value[..8]);
                let tx_index = i64::from_be_bytes(bytes);
                conn.execute(
                    "INSERT OR REPLACE INTO transaction_index (tx_hash, tx_index) values (?1, ?2)",
                    &[&key as &dyn ToSql, &tx_index as &dyn ToSql],
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
                if key.len() != 32 {
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
            5 => {
                if key.len() != 8 {
                    return Err("len of key is not correct".to_owned());
                }
                let mut bytes: [u8; 8] = [0; 8];
                bytes[..8].clone_from_slice(&key[..8]);
                let block_height = i64::from_be_bytes(bytes);
                conn.query_row(
                    "SELECT proof FROM proofs WHERE block_height=?",
                    &[&block_height],
                    |row| row.get(0),
                )
            }
            6 => {
                if key.len() != 8 {
                    return Err("len of key is not correct".to_owned());
                }
                let mut bytes: [u8; 8] = [0; 8];
                bytes[..8].clone_from_slice(&key[..8]);
                let block_height = i64::from_be_bytes(bytes);
                conn.query_row(
                    "SELECT executed_block_hash FROM results WHERE block_height=?",
                    &[&block_height],
                    |row| row.get(0),
                )
            }
            7 => {
                if key.len() != 32 {
                    return Err("len of key is not correct".to_owned());
                }

                conn.query_row(
                    "SELECT block_height FROM tx_hash_2_block_height WHERE tx_hash=?",
                    &[&key],
                    |row| row.get(0).map(|v: i64| v.to_be_bytes().to_vec()),
                )
            }
            8 => {
                if key.len() != 32 {
                    return Err("len of key is not correct".to_owned());
                }
                conn.query_row(
                    "SELECT block_height FROM blockhash WHERE block_hash=?",
                    &[&key],
                    |row| row.get(0).map(|v: i64| v.to_be_bytes().to_vec()),
                )
            }
            9 => {
                if key.len() != 32 {
                    return Err("len of key is not correct".to_owned());
                }
                conn.query_row(
                    "SELECT tx_index FROM transaction_index WHERE tx_hash=?",
                    &[&key],
                    |row| row.get(0).map(|v: i64| v.to_be_bytes().to_vec()),
                )
            }
            _ => return Err("id is not correct".to_owned()),
        };
        if ret == Err(Error::QueryReturnedNoRows) {
            Err("not found".to_owned())
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
                if key.len() != 32 {
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
            5 => {
                if key.len() != 8 {
                    return Err("len of key is not correct".to_owned());
                }
                let mut bytes: [u8; 8] = [0; 8];
                bytes[..8].clone_from_slice(&key[..8]);
                let block_height = i64::from_be_bytes(bytes);
                conn.execute("DELETE FROM proofs WHERE block_height=?", &[&block_height])
            }
            6 => {
                if key.len() != 8 {
                    return Err("len of key is not correct".to_owned());
                }
                let mut bytes: [u8; 8] = [0; 8];
                bytes[..8].clone_from_slice(&key[..8]);
                let block_height = i64::from_be_bytes(bytes);
                conn.execute("DELETE FROM results WHERE block_height=?", &[&block_height])
            }
            7 => {
                if key.len() != 32 {
                    return Err("len of key is not correct".to_owned());
                }
                conn.execute(
                    "DELETE FROM tx_hash_2_block_height WHERE tx_hash=?",
                    &[&key],
                )
            }
            8 => {
                if key.len() != 32 {
                    return Err("len of key is not correct".to_owned());
                }
                conn.execute("DELETE FROM blockhash WHERE block_hash=?", &[&key])
            }
            9 => {
                if key.len() != 32 {
                    return Err("len of key is not correct".to_owned());
                }
                conn.execute("DELETE FROM transaction_index WHERE tx_hash=?", &[&key])
            }
            _ => return Err("id is not correct".to_owned()),
        };
        ret.map(|_| ())
            .map_err(|e| format!("delete error: {:?}", e))
    }
}

#[cfg(test)]
mod tests {
    use super::DB;
    use quickcheck::quickcheck;
    use quickcheck::Arbitrary;
    use quickcheck::Gen;
    use tempfile::NamedTempFile;

    #[derive(Clone, Debug)]
    struct DBTestArgs {
        region: u32,
        key: Vec<u8>,
        value: Vec<u8>,
    }

    impl Arbitrary for DBTestArgs {
        fn arbitrary(g: &mut Gen) -> Self {
            let region = u32::arbitrary(g) % 10;
            let key = match region {
                1 | 7 | 8 | 9 => {
                    let mut k = Vec::with_capacity(32);
                    for _ in 0..4 {
                        let bytes = u64::arbitrary(g).to_be_bytes().to_vec();
                        k.extend_from_slice(&bytes);
                    }
                    k
                }
                _ => {
                    let mut k = Vec::with_capacity(8);
                    let bytes = u64::arbitrary(g).to_be_bytes().to_vec();
                    k.extend_from_slice(&bytes);
                    k
                }
            };

            let value = match region {
                7 | 8 | 9 => {
                    let mut v = Vec::with_capacity(8);
                    let bytes = u64::arbitrary(g).to_be_bytes().to_vec();
                    v.extend_from_slice(&bytes);
                    v
                }
                _ => {
                    let mut v = Vec::with_capacity(32);
                    for _ in 0..4 {
                        let bytes = u64::arbitrary(g).to_be_bytes().to_vec();
                        v.extend_from_slice(&bytes);
                    }
                    v
                }
            };

            DBTestArgs { region, key, value }
        }
    }

    quickcheck! {
         fn prop(args: DBTestArgs) -> bool {
             let tmpfile: NamedTempFile = NamedTempFile::new().unwrap();
             let path = tmpfile.path().to_str().unwrap();
             let db = DB::new(path);

             let region = args.region;
             let key = args.key.clone();
             let value = args.value;

             db.store(region, key.clone(), value.clone()).unwrap();
             db.load(region, key).unwrap() == value
         }
    }
}
