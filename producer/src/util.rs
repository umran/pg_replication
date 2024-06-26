use std::collections::HashMap;

use anyhow::{anyhow, bail};
use openssl::ssl::{SslConnector, SslFiletype, SslMethod, SslVerifyMode};
use postgres_openssl::MakeTlsConnector;
use serde::{Deserialize, Serialize};
use tokio_postgres::config::{ReplicationMode, SslMode};
use tokio_postgres::types::Type as PgType;
use tokio_postgres::{Client, Config};

use crate::error::ReplicationError;

mod task;

#[derive(Serialize, Deserialize)]
pub struct TopicInfo {
    pub name: String,
    pub partition_key: Vec<String>,
}

/// Information about a remote table
#[derive(Debug)]
pub struct TableInfo {
    /// The OID of the table
    pub rel_id: u32,
    /// The namespace the table belongs to
    pub namespace: String,
    /// The name of the table
    pub name: String,
    /// The topic to publish this table to
    pub topic: String,
    /// The schema of each column, in order
    pub schema: Vec<Column>,
    /// The list of column names, in the expected order for
    /// decoding logical replication
    pub col_names: Vec<String>,
}

/// Information about a table column
#[derive(Debug)]
pub struct Column {
    pub name: String,
    pub pg_type: PgType,
    pub nullable: bool,
    pub primary_key: bool,
    /// Indicates whether this column is part of the partition_key used when publishing to Kafka
    pub partition_key: bool,
}

impl TableInfo {
    pub fn extract_partition_key(
        &self,
        row: &Vec<Option<String>>,
    ) -> Result<Vec<String>, anyhow::Error> {
        let partition_key_indices = self
            .schema
            .iter()
            .enumerate()
            .filter_map(|(i, col)| if col.partition_key { Some(i) } else { None })
            .collect::<Vec<_>>();

        let mut partition_key = vec![];

        for i in partition_key_indices.into_iter() {
            let value = row.get(i).ok_or_else(|| {
                anyhow!("row does not have partition key index as defined in schema")
            })?;
            let value = value.clone().ok_or_else(|| {
                anyhow!(
                    "a partition key column of the row (according to the schema) happens to be null"
                )
            })?;

            partition_key.push(value);
        }

        Ok(partition_key)
    }
}

/// Creates a TLS connector for the given [`Config`].
fn make_tls(config: &Config) -> Result<MakeTlsConnector, anyhow::Error> {
    let mut builder = SslConnector::builder(SslMethod::tls_client())?;
    // The mode dictates whether we verify peer certs and hostnames. By default, Postgres is
    // pretty relaxed and recommends SslMode::VerifyCa or SslMode::VerifyFull for security.
    //
    // For more details, check out Table 33.1. SSL Mode Descriptions in
    // https://postgresql.org/docs/current/libpq-ssl.html#LIBPQ-SSL-PROTECTION.
    let (verify_mode, verify_hostname) = match config.get_ssl_mode() {
        SslMode::Disable | SslMode::Prefer => (SslVerifyMode::NONE, false),
        SslMode::Require => match config.get_ssl_root_cert() {
            // If a root CA file exists, the behavior of sslmode=require will be the same as
            // that of verify-ca, meaning the server certificate is validated against the CA.
            //
            // For more details, check out the note about backwards compatibility in
            // https://postgresql.org/docs/current/libpq-ssl.html#LIBQ-SSL-CERTIFICATES.
            Some(_) => (SslVerifyMode::PEER, false),
            None => (SslVerifyMode::NONE, false),
        },
        SslMode::VerifyCa => (SslVerifyMode::PEER, false),
        SslMode::VerifyFull => (SslVerifyMode::PEER, true),
        _ => panic!("unexpected sslmode {:?}", config.get_ssl_mode()),
    };

    // Configure peer verification
    builder.set_verify(verify_mode);

    // Configure certificates
    match (config.get_ssl_cert(), config.get_ssl_key()) {
        (Some(ssl_cert), Some(ssl_key)) => {
            builder.set_certificate_file(ssl_cert, SslFiletype::PEM)?;
            builder.set_private_key_file(ssl_key, SslFiletype::PEM)?;
        }
        (None, Some(_)) => bail!("must provide both sslcert and sslkey, but only provided sslkey"),
        (Some(_), None) => bail!("must provide both sslcert and sslkey, but only provided sslcert"),
        _ => {}
    }
    if let Some(ssl_root_cert) = config.get_ssl_root_cert() {
        builder.set_ca_file(ssl_root_cert)?
    }

    let mut tls_connector = MakeTlsConnector::new(builder.build());

    // Configure hostname verification
    match (verify_mode, verify_hostname) {
        (SslVerifyMode::PEER, false) => tls_connector.set_callback(|connect, _| {
            connect.set_verify_hostname(false);
            Ok(())
        }),
        _ => {}
    }

    Ok(tls_connector)
}

/// Fetches table schema information from an upstream Postgres source for all tables that are part
/// of a publication, given a connection string and the publication name.
///
/// # Errors
///
/// - Invalid connection string, user information, or user permissions.
/// - Upstream publication does not exist or contains invalid values.
pub async fn publication_info(
    client: &Client,
    publication: &str,
    topic_map: &HashMap<String, TopicInfo>,
) -> Result<HashMap<u32, TableInfo>, ReplicationError> {
    client
        .query(
            "SELECT oid FROM pg_publication WHERE pubname = $1",
            &[&publication],
        )
        .await?
        .get(0)
        .ok_or_else(|| {
            ReplicationError::Fatal(anyhow!("publication {:?} does not exist", publication))
        })?;

    let tables = client
        .query(
            "SELECT
                c.oid, p.schemaname, p.tablename
            FROM
                pg_catalog.pg_class AS c
                JOIN pg_namespace AS n ON c.relnamespace = n.oid
                JOIN pg_publication_tables AS p ON
                        c.relname = p.tablename AND n.nspname = p.schemaname
            WHERE
                p.pubname = $1",
            &[&publication],
        )
        .await?;

    let mut table_infos = HashMap::new();
    for row in tables {
        let rel_id = row.get("oid");

        let namespace = row.get("schemaname");
        let name = row.get("tablename");

        let topic_info = topic_map
            .get(&name)
            .ok_or_else(|| ReplicationError::Fatal(anyhow!("TopicInfo missing for table")))?;

        // check that there is at least one partition_key column defined in TopicInfo
        // 1-1 correspondence between columns defined in TopicInfo and the table schema
        // is checked below
        if topic_info.partition_key.len() == 0 {
            return Err(ReplicationError::Fatal(anyhow!(
                "at least one partition_key column must be defined in TopicInfo"
            )));
        }

        let schema = client
            .query(
                "SELECT
                        a.attname AS name,
                        a.atttypid AS oid,
                        a.atttypmod AS modifier,
                        a.attnotnull AS not_null,
                        b.oid IS NOT NULL AS primary_key
                    FROM pg_catalog.pg_attribute a
                    LEFT JOIN pg_catalog.pg_constraint b
                        ON a.attrelid = b.conrelid
                        AND b.contype = 'p'
                        AND a.attnum = ANY (b.conkey)
                    WHERE a.attnum > 0::pg_catalog.int2
                        AND NOT a.attisdropped
                        AND a.attrelid = $1
                    ORDER BY a.attnum",
                &[&rel_id],
            )
            .await?
            .into_iter()
            .map(|row| {
                let name: String = row.get("name");
                let oid = row.get("oid");
                let pg_type = PgType::from_oid(oid)
                    .ok_or_else(|| ReplicationError::Fatal(anyhow!("unknown type OID: {}", oid)))?;
                let not_null: bool = row.get("not_null");
                let nullable = !not_null;
                let primary_key = row.get("primary_key");
                let partition_key = topic_info.partition_key.contains(&name);

                // if the column is declared a partition_key column, but is nullable
                // it is invalid
                if partition_key && nullable {
                    return Err(ReplicationError::Fatal(anyhow!(
                        "column is declared a partition_key, but is also declared as nullable"
                    )));
                }

                Ok(Column {
                    name,
                    pg_type,
                    nullable,
                    primary_key,
                    partition_key,
                })
            })
            .collect::<Result<Vec<_>, ReplicationError>>()?;

        // we must validate the partition_key columns of schema such that there
        // are exactly as many partition_key columns as are defined in TopicInfo
        if topic_info.partition_key.len()
            != schema
                .iter()
                .filter(|col| col.partition_key)
                .collect::<Vec<_>>()
                .len()
        {
            return Err(ReplicationError::Fatal(anyhow!("at least one column defined as a partition key in TopicInfo does not exist on the table")));
        }

        let col_names = schema.iter().map(|col| col.name.clone()).collect();

        table_infos.insert(
            rel_id,
            TableInfo {
                rel_id,
                namespace,
                name,
                schema,
                col_names,
                topic: topic_info.name.clone(),
            },
        );
    }

    Ok(table_infos)
}

/// Starts a replication connection to the upstream database
pub async fn connect_replication(conn: &str) -> Result<Client, anyhow::Error> {
    let mut config: Config = conn.parse()?;
    let tls = make_tls(&config)?;
    let (client, connection) = config
        .replication_mode(ReplicationMode::Logical)
        .connect(tls)
        .await?;
    task::spawn(
        || format!("postgres_connect_replication:{conn}"),
        connection,
    );
    Ok(client)
}

/// Starts a replication connection to the upstream database
pub async fn connect_basic(conn: &str) -> Result<Client, anyhow::Error> {
    let config: Config = conn.parse()?;
    let tls = make_tls(&config)?;

    let (client, connection) = tokio_postgres::connect(conn, tls).await?;
    task::spawn(|| format!("postgres_connect_basic:{conn}"), connection);

    Ok(client)
}
