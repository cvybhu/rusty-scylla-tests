use futures::StreamExt;
use rand::Rng;
use scylla::{
    frame::value::ValueList,
    prepared_statement::PreparedStatement,
    query::Query,
    transport::Node,
    transport::{errors::QueryError, ClusterData},
    Session, SessionBuilder,
};
use std::{sync::Arc, time::Duration};

pub struct RunStats {
    pub concurrency: usize,
    pub requests_done: usize,
    pub duration: Duration,
}

impl RunStats {
    pub fn requests_per_second(&self) -> f64 {
        let total_time_in_secs: f64 = self.duration.as_secs_f64();

        if total_time_in_secs == 0.0 {
            return 0.0;
        }

        self.requests_done as f64 / total_time_in_secs
    }
}

impl std::fmt::Display for RunStats {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "RunStats(concurrency: {}, requests/second: {:.1}, total requests: {}, duration: {:?})",
            self.concurrency,
            self.requests_per_second(),
            self.requests_done,
            self.duration
        )
    }
}

pub async fn run_with_concurrency<ValuesType: ValueList>(
    session: &Session,
    query: impl Into<Query>,
    query_values: impl Iterator<Item = ValuesType>,
    concurrency: usize,
) -> Result<RunStats, QueryError> {
    let prepared: PreparedStatement = session.prepare(query).await?;

    let query_futures = query_values.map(|values| session.execute(&prepared, values));

    let mut queries_stream = futures::stream::iter(query_futures).buffer_unordered(concurrency);

    let start_time: std::time::Instant = std::time::Instant::now();
    let mut requests_done: usize = 0;

    while let Some(result) = queries_stream.next().await {
        match result {
            Ok(_) => requests_done += 1,
            Err(_query_error) => return Err(_query_error),
        }
    }

    Ok(RunStats {
        concurrency,
        requests_done,
        duration: start_time.elapsed(),
    })
}

pub async fn run_max_concurrency<ValuesType: ValueList>(
    session: &Session,
    query: impl Into<Query>,
    query_values: impl Iterator<Item = ValuesType>,
) -> Result<RunStats, QueryError> {
    run_with_concurrency(session, query, query_values, 14_000).await
}

pub fn random_keyspace_name() -> String {
    let random_u64: u64 = rand::thread_rng().gen();

    format!("keyspace_{random_u64}")
}

pub fn random_table_name() -> String {
    let random_u64: u64 = rand::thread_rng().gen();

    format!("table_{random_u64}")
}

pub async fn new_session(nodes: &[String]) -> Session {
    let mut builder = SessionBuilder::new();
    for node in nodes {
        builder = builder.known_node(node);
    }
    builder.build().await.unwrap()
}

pub async fn new_session_with_simple_keyspace(
    nodes: &[String],
    replication_factor: usize,
) -> (Session, String) {
    let session: Session = new_session(nodes).await;

    let ks_name: String = random_keyspace_name();

    let query_str = format!("CREATE KEYSPACE {} WITH replication = {{'class': 'SimpleStrategy', 'replication_factor': {}}}", ks_name, replication_factor);
    println!("{}", query_str);
    session.query(query_str, ()).await.unwrap();

    session.use_keyspace(ks_name.clone(), true).await.unwrap();

    (session, ks_name)
}

pub async fn wait_for_view(session: &Session, base_table: &str, view_table: &str) {
    async fn prepare_count_query(session: &Session, table_name: &str) -> PreparedStatement {
        let mut prepared: PreparedStatement = session
            .prepare(format!(
                "SELECT count(*) FROM {table_name} USING TIMEOUT 1h"
            ))
            .await
            .unwrap();

        prepared.set_request_timeout(Some(Duration::from_secs(3600)));

        prepared
    }

    let (base_count_query, view_count_query) = futures::join!(
        prepare_count_query(session, base_table),
        prepare_count_query(session, view_table)
    );

    async fn get_row_count_for_table(session: &Session, get_count_stmt: &PreparedStatement) -> i64 {
        let count: i64 = session
            .execute(&get_count_stmt, ())
            .await
            .unwrap()
            .first_row_typed::<(i64,)>()
            .unwrap()
            .0;

        count
    }

    loop {
        let (base_count, view_count) = futures::join!(
            get_row_count_for_table(session, &base_count_query),
            get_row_count_for_table(session, &view_count_query)
        );

        if base_count == view_count {
            return;
        }

        tokio::time::sleep(Duration::from_millis(200)).await;
    }
}

pub async fn wait_for_many_views<ViewName>(
    session: &Session,
    base_table: &str,
    view_tables: impl Iterator<Item = ViewName>,
    concurrency: usize,
) where
    ViewName: AsRef<str>,
{
    // Wait for views to sync
    println!("Waiting for views to sync...");
    let wait_for_view_futures = view_tables.map(|view_name| {
        let session_ref = &session;
        async move {
            wait_for_view(session_ref, base_table, view_name.as_ref()).await;
        }
    });

    futures::stream::iter(wait_for_view_futures)
        .buffer_unordered(concurrency)
        .count()
        .await;
}

pub fn get_random_pk_for_node(
    node: &Arc<Node>,
    keyspace: &str,
    table: &str,
    cluster_data: &ClusterData,
) -> i64 {
    loop {
        let random_i64: i64 = rand::thread_rng().gen();
        let nodes = cluster_data
            .get_endpoints(keyspace, table, (random_i64,))
            .unwrap();
        if nodes.contains(node) {
            return random_i64;
        }
    }
}

pub async fn delete_where_pk_in(session: &Session, table: &str, pks: impl Iterator<Item = i64>) {
    let mut query = Query::new(format!(
        "DELETE FROM {table} USING TIMEOUT 1h WHERE pk IN ?"
    ));
    query.set_request_timeout(Some(Duration::from_secs(3600)));

    let mut in_values: Vec<Vec<i64>> = Vec::new();
    let mut cur_values: Vec<i64> = Vec::new();
    for pk in pks {
        if cur_values.len() >= 100 {
            in_values.push(cur_values);
            cur_values = Vec::new();
        }

        cur_values.push(pk);
    }

    if !cur_values.is_empty() {
        in_values.push(cur_values);
    }

    for value_list in &in_values {
        println!(
            "DELETE FROM {table} WHERE pk IN {:?}",
            format_list_short(value_list.iter())
        );
    }

    run_with_concurrency(
        session,
        query,
        in_values.into_iter().map(|in_values| (in_values,)),
        128,
    )
    .await
    .unwrap();
}

// formats a list of elements in a shortened form.
// for example for a list of 0..100 the output would be:
// (0, 1, 2, 3, 4, [skipping 94 elements], 99)
fn format_list_short<E: std::fmt::Display>(elems: impl Iterator<Item = E>) -> String {
    let mut result: String = "(".to_string();

    let mut skipped_elements: usize = 0;
    let mut last_element: Option<E> = None;

    for (i, elem) in elems.enumerate() {
        if i < 5 {
            if i == 0 {
                result += &format!("{}", elem);
            } else {
                result += &format!(", {}", elem);
            }
        } else {
            if last_element.is_some() {
                skipped_elements += 1;
            }

            last_element = Some(elem);
        }
    }

    match last_element {
        Some(elem) => format!("{result}, [skipping {skipped_elements} elements], {elem})"),
        None => result + ")",
    }
}
