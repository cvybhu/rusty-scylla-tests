// A test in which all of the view updates are sent to one node.
// It creates a table and many materialized views based on this table.
// Then it inserts a lot of rows, chosen in a way so that the base table is
// spread evenly among n-1 nodes, but all of those rows belong to a single
// materialized view partition, which is stored on the nth node.
// This way all of the view updates are remote, and they're sent
// to a single node that doesn't generate any view updates of its own.

use crate::utils;

use scylla::{
    query::Query,
    transport::{Node, NodeAddr},
};
use std::{sync::Arc, time::Duration};

/// Arguments:
/// nodes - addresses of nodes to connect to
/// pk_count - number of partitions to create
/// views_count - number of materialized views to create
/// workload_rows - total number of rows that the database will store. workload_rows_number = inserted_rows * (1 + views_count)
/// insert_concurrency - concurrency used for inserting values
pub async fn many_view_updates_to_one_node(
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

    let cluster_data = session.get_cluster_data();
    let nodes: &[Arc<Node>] = cluster_data.get_nodes_info();

    if nodes.len() < 2 {
        panic!("This test is meant to be run with at least 2 nodes!");
    }

    // Find out which nodes will send the updates, and which one will receive them
    let sender_nodes: &[Arc<Node>] = &nodes[..(nodes.len() - 1)];
    let receiver_node: &Arc<Node> = nodes.last().unwrap();

    println!(
        "sender_nodes: {:?}",
        sender_nodes
            .iter()
            .map(|n| n.address)
            .collect::<Vec<NodeAddr>>()
    );
    println!("receiver_node: {:?}\n", receiver_node.address);

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

    // Create pk_count pks that belong to sender nodes
    let mut sender_pks: Vec<i64> = Vec::new();

    'outer: loop {
        for node in sender_nodes {
            if sender_pks.len() == pk_count as usize {
                break 'outer;
            }

            let sender_node_pk =
                utils::get_random_pk_for_node(node, &ks_name, "tab", &cluster_data);
            sender_pks.push(sender_node_pk);
        }
    }

    // Create one pk that will belong to the receiver node
    let receiver_node_pk: i64 =
        utils::get_random_pk_for_node(receiver_node, &ks_name, "tab", &cluster_data);

    // iterator of (pk, ck, r) to insert.
    // cks go 0..
    // pks are sender pks
    // r = receiver_node_pk
    let rows_to_insert: usize = workload_rows / (views_count + 1);
    let values_to_insert = (0..)
        .map(|ck_val: i64| {
            let pk_val = sender_pks[ck_val as usize % sender_pks.len()];
            (pk_val, ck_val, receiver_node_pk)
        })
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
    utils::wait_for_many_views(&session, "tab", view_names.clone(), 4).await;

    println!("Deleting {pk_count} partitions...");
    utils::delete_where_pk_in(&session, "tab", 0..(pk_count as i64)).await;

    // Wait for views to sync again after the deletion.
    utils::wait_for_many_views(&session, "tab", view_names, 4).await;
    println!("OK");

    // Drop the keyspace so that it doesn't take up space and resources.
    let drop_ks_str = format!("DROP KEYSPACE {ks_name}");
    println!("{}", drop_ks_str);
    session.query(drop_ks_str, ()).await.unwrap();
}
