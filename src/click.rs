use clickhouse::{Client, Row};
use std::env;

use serde::{Deserialize, Serialize};

use fastnear_primitives::near_primitives::types::BlockHeight;
use std::time::Duration;

pub const CLICKHOUSE_TARGET: &str = "clickhouse";
pub const MAX_COMMIT_HANDLERS: usize = 3;

/// Timeout for streaming one chunk of an INSERT body to the server.
const DEFAULT_INSERT_SEND_TIMEOUT_SECS: u64 = 30;
/// Timeout for the server to acknowledge the whole INSERT. This covers all the work the
/// server does for it: materialized views, replication, and -- with
/// `insert_distributed_sync` -- the fan-out to the shards. Sized generously because
/// bundling makes batches much larger than they used to be.
const DEFAULT_INSERT_END_TIMEOUT_SECS: u64 = 120;

#[derive(Row, Deserialize, Debug)]
pub struct BlockChainRow {
    pub block_height: BlockHeight,
    pub prev_block_height: Option<BlockHeight>,
}

#[derive(Clone)]
pub struct ClickDB {
    pub client: Client,
    pub skip_commit: bool,
    pub send_timeout: Duration,
    pub end_timeout: Duration,
}

impl ClickDB {
    pub fn new() -> Self {
        let skip_commit = env_flag("CLICKHOUSE_SKIP_COMMIT", false);
        if skip_commit {
            tracing::log::warn!(target: CLICKHOUSE_TARGET, "CLICKHOUSE_SKIP_COMMIT is set: no rows will be written");
        }
        Self {
            client: establish_connection(),
            skip_commit,
            send_timeout: Duration::from_secs(env_u64(
                "CLICKHOUSE_INSERT_SEND_TIMEOUT_SECS",
                DEFAULT_INSERT_SEND_TIMEOUT_SECS,
            )),
            end_timeout: Duration::from_secs(env_u64(
                "CLICKHOUSE_INSERT_END_TIMEOUT_SECS",
                DEFAULT_INSERT_END_TIMEOUT_SECS,
            )),
        }
    }

    pub async fn max_in_range(
        &self,
        column: &str,
        table: &str,
        start_block: BlockHeight,
        end_block: BlockHeight,
    ) -> clickhouse::error::Result<BlockHeight> {
        let block_height = self
            .client
            .query(&format!(
                "SELECT max({column}) FROM {table} where block_height >= {start_block} and block_height < {end_block}"
            ))
            .fetch_one::<u64>()
            .await?;
        Ok(block_height)
    }

    /// The `(block_height, prev_block_height)` chain over a range, ascending.
    ///
    /// Block heights are not consecutive -- the chain legitimately skips heights with no
    /// block -- so `prev_block_height` is the only way to tell a real hole from a skipped
    /// height.
    pub async fn fetch_block_chain(
        &self,
        start_block: BlockHeight,
        end_block: BlockHeight,
    ) -> clickhouse::error::Result<Vec<BlockChainRow>> {
        self.client
            .query(&format!(
                "SELECT block_height, prev_block_height FROM blocks \
                 WHERE block_height >= {start_block} AND block_height <= {end_block} \
                 ORDER BY block_height"
            ))
            .fetch_all::<BlockChainRow>()
            .await
    }

    pub async fn verify_connection(&self) -> clickhouse::error::Result<()> {
        self.client.query("SELECT 1").execute().await?;
        Ok(())
    }
}

pub fn env_flag(name: &str, default: bool) -> bool {
    env::var(name)
        .ok()
        .map(|v| v == "true" || v == "1")
        .unwrap_or(default)
}

pub fn env_u64(name: &str, default: u64) -> u64 {
    env::var(name)
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(default)
}

fn establish_connection() -> Client {
    let mut client = Client::default()
        .with_url(env::var("DATABASE_URL").unwrap())
        .with_user(env::var("DATABASE_USER").unwrap())
        .with_password(env::var("DATABASE_PASSWORD").unwrap())
        .with_database(env::var("DATABASE_DATABASE").unwrap());
    // All the tables we write to are `Distributed`. By default an INSERT into a
    // `Distributed` table is acknowledged as soon as the coordinator has written its
    // temporary files, *not* once the shards actually hold the rows. The ordered
    // watermark in `TransactionsData::commit` relies on an acknowledged `blocks` INSERT
    // being durable, so ask for a foreground insert instead.
    if env_flag("CLICKHOUSE_DISTRIBUTED_SYNC", true) {
        client = client.with_option("insert_distributed_sync", "1");
    }
    client
}

pub async fn insert_rows_with_retry<T>(
    db: &ClickDB,
    rows: &[T],
    table: &str,
) -> clickhouse::error::Result<()>
where
    T: Row + Serialize,
{
    if rows.is_empty() || db.skip_commit {
        return Ok(());
    }
    let mut delay = Duration::from_millis(100);
    let max_retries = 10;
    let mut i = 0;
    loop {
        let res = async {
            // Native timeouts, rather than wrapping the futures in `tokio::time::timeout`:
            // the crate documents them as ~10x cheaper, and without them a half-open
            // connection wedges the commit task (and its semaphore permit) forever.
            let mut insert = db
                .client
                .insert(table)?
                .with_timeouts(Some(db.send_timeout), Some(db.end_timeout));
            for row in rows {
                insert.write(row).await?;
            }
            insert.end().await
        };
        match res.await {
            Ok(v) => break Ok(v),
            Err(err) => {
                if i == max_retries - 1 {
                    tracing::log::error!(target: CLICKHOUSE_TARGET, "Giving up after {} attempts inserting {} rows into \"{}\": {}", max_retries, rows.len(), table, err);
                    break Err(err);
                }
                // Not an error yet -- this attempt will be retried. A stale pooled
                // keep-alive connection ("client error (SendRequest)") shows up here
                // routinely and succeeds on the next attempt.
                tracing::log::warn!(target: CLICKHOUSE_TARGET, "Attempt #{}: Error inserting {} rows into \"{}\": {}", i, rows.len(), table, err);
                tokio::time::sleep(delay).await;
                delay *= 2;
            }
        };
        i += 1;
    }
}
