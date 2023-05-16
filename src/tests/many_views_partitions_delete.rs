// A test that creates many materialized views, fills them up,
// and then deletes all partitions, causing a flurry of view updates.

use std::time::Duration;

use crate::utils::{self, wait_for_many_views};
use scylla::query::Query;

/// Arguments:
/// nodes - Addresses of nodes to connect to
/// pk_count - number of partitions to create
/// views_count - number of materialized views to create
/// workload_rows - total number of rows that the database will store. workload_rows_number = inserted_rows * (1 + views_count)
/// insert_concurrency - concurrency used for inserting values
pub async fn many_views_partitions_delete(
    nodes: &[String],
    pk_count: usize,
    views_count: usize,
    workload_rows: usize,
    insert_concurrency: usize,
) {
    // Print configuration
    println!("Configuration:");
    println!("nodes: {:?}", nodes);
    println!("pk_count: {}", pk_count);
    println!("views_count: {}", views_count);
    println!("workload_rows: {}", workload_rows);
    println!("insert_concurrency: {}", insert_concurrency);
    println!("");

    // Connect to the cluster
    let replication_factor = 1;
    let (session, ks_name) =
        utils::new_session_with_simple_keyspace(nodes, replication_factor).await;

    // Create the table
    let create_table_str =
        "CREATE TABLE tab (pk bigint, ck bigint, r bigint, PRIMARY KEY (pk, ck))";
    println!("{}", create_table_str);
    session.query(create_table_str, ()).await.unwrap();

    // Create the materialized views
    let view_names = (0..views_count).map(|view_idx: usize| format!("tab_view{view_idx}"));

    for (i, view_name) in view_names.clone().enumerate() {
        let create_view_str =
            format!("CREATE MATERIALIZED VIEW {view_name} AS SELECT pk, ck, r FROM tab \
                     WHERE pk IS NOT NULL and ck IS NOT NULL and r IS NOT NULL PRIMARY KEY (r, ck, pk)");
        if i < 3 || i + 1 == views_count {
            println!("{}", create_view_str)
        } else if i == 3 {
            println!("...")
        }

        session.query(create_view_str, ()).await.unwrap();
    }

    // iterator of (pk, ck, r) to insert.
    // cks go 0..
    // pks go 0..pk_count
    // r = pk + ck
    let rows_to_insert: usize = workload_rows / (views_count + 1);
    let values_to_insert = (0..)
        .map(|ck_val: i64| (0..pk_count).map(move |pk_val: usize| (pk_val as i64, ck_val)))
        .flatten()
        .map(|(pk_val, ck_val): (i64, i64)| (pk_val, ck_val, pk_val + ck_val))
        .take(rows_to_insert);

    println!(
        "Inserting {} rows... (workload is {} rows)",
        rows_to_insert, workload_rows
    );

    let mut insert_query: Query =
        "INSERT INTO tab (pk, ck, r) VALUES (?, ?, ?) USING TIMEOUT 1h".into();
    insert_query.set_request_timeout(Some(Duration::from_secs(3600)));

    let insert_stats =
        utils::run_with_concurrency(&session, insert_query, values_to_insert, insert_concurrency)
            .await
            .unwrap();

    println!("Insertion stats: {}", insert_stats);

    // Wait for views to sync after all the inserts.
    wait_for_many_views(&session, "tab", view_names.clone(), 4).await;

    println!("Deleting {pk_count} partitions...");
    utils::delete_where_pk_in(&session, "tab", 0..(pk_count as i64)).await;

    // Wait for views to sync again after the deletion.
    wait_for_many_views(&session, "tab", view_names, 4).await;
    println!("OK");

    // Drop the keyspace so that it doesn't take up space and resources.
    let drop_ks_str = format!("DROP KEYSPACE {ks_name}");
    println!("{}", drop_ks_str);
    session.query(drop_ks_str, ()).await.unwrap();
}
