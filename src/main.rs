use scylla::batch::Batch;
use scylla::statement::Consistency;
use scylla::*;
use std::collections::HashSet;
use std::error::Error;
use std::sync::Arc;

const HOST: &str = "TODO";
const READ_CONSISTENCY: Consistency = Consistency::Quorum;

#[tokio::main]
async fn main() -> Result<(), Box<dyn Error>> {
    let session = Arc::new(
        SessionBuilder::new()
            .known_node(HOST)
            .use_keyspace("experiments", false)
            .build()
            .await?,
    );

    session.query("CREATE KEYSPACE IF NOT EXISTS experiments WITH replication = { 'class': 'SimpleStrategy', 'replication_factor': 2 }", ()).await?;
    session.query("DROP TABLE IF EXISTS experiments.isolation_test", ()).await?;
    session.query("CREATE TABLE experiments.isolation_test (pk int, id int, version bigint, PRIMARY KEY (pk, id))", ()).await?;

    // Initialize with (1, 0..100, 0)
    for i in 0..100 {
        session
            .query("INSERT INTO experiments.isolation_test (pk, id, version) VALUES (1, ?, 0)", (i, ))
            .await?;
    }

    // 10 readers in parallel reading the table
    // the versions they receive should always be the same
    let mut handles = Vec::new();
    for _ in 0..10 {
        handles.push(tokio::spawn({
            let session = session.clone();
            async move {
                let mut select = session
                    .prepare("SELECT version FROM isolation_test")
                    .await
                    .unwrap();
                select.set_consistency(READ_CONSISTENCY);
                loop {
                    if let Some(rows) = session.execute(&select, ()).await.unwrap().rows {
                        let numbers: HashSet<i64> = rows.into_typed::<(i64, )>().map(|r| r.unwrap().0).collect();
                        if numbers.len() > 1 {
                            panic!("GOT DIFFERENT VERSIONS {:?}", numbers);
                        }
                    }
                }
            }
        }));
    }

    // 10 writers in parallel writing to the table, every writer updating all rows of partition 1 to the same version
    // using a separate version per writer
    for i in 0..10 {
        handles.push(tokio::spawn({
            let session = session.clone();
            async move {
                let mut current = 0i64;
                let insert1 = session
                    .prepare("UPDATE isolation_test SET version = ? WHERE pk = 1 AND id = ? IF EXISTS")
                    .await
                    .unwrap();
                loop {
                    let mut batch: Batch = Default::default();
                    let mut updates = Vec::new();
                    for i in 0..100 {
                        batch.append_statement(insert1.clone());
                        updates.push((current, i));
                    }
                    if let Err(err) = session.batch(&batch, updates).await {
                        println!("FAILED {} {}", i, err);
                    }
                    current += i;
                }
            }
        }));
    }

    for h in handles {
        h.await?;
    }

    Ok(())
}
