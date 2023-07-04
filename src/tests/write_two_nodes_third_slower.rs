use histogram::Histogram;
use rand::Rng;
use scylla::_macro_internal::ValueList;
use scylla::{
    host_filter::AllowListHostFilter, prepared_statement::PreparedStatement,
    statement::Consistency, Session, SessionBuilder,
};
use std::time::Instant;
use std::{net::ToSocketAddrs, sync::Arc, time::Duration};

struct LatencyData {
    latency_histogram: Histogram,
}

impl LatencyData {
    pub fn new() -> LatencyData {
        LatencyData {
            latency_histogram: Histogram::new(0, 10, 30).unwrap(),
        }
    }

    pub fn add_latency(&mut self, query_latency: Duration) {
        let micros: u128 = query_latency.as_micros();
        let micros_u64: u64 = match micros.try_into() {
            Ok(micros_u64) => micros_u64,
            Err(_) => {
                println!("Humongous latency: {:?}", query_latency);
                return;
            }
        };

        if let Err(e) = self.latency_histogram.increment(micros_u64, 1) {
            println!("Error adding latency: {:?} error: {}", query_latency, e);
        }
    }

    pub fn print_percentiles(&self, percentiles: &[f64]) {
        let mut sorted_percentiles = percentiles.to_vec();
        sorted_percentiles.sort_by(|a, b| a.partial_cmp(b).unwrap());

        let latencies: Vec<u64> = self
            .latency_histogram
            .percentiles(&sorted_percentiles)
            .unwrap()
            .into_iter()
            .map(|p| p.bucket().high())
            .collect();
        for i in 0..sorted_percentiles.len() {
            let percentile = sorted_percentiles[i] * 100.0;
            let latency_ms: f64 = latencies[i] as f64 / 1000.0;

            print!("({percentile}% latency: {latency_ms:.1}ms)");
        }
    }

    pub fn clear(&mut self) {
        *self = Self::new();
    }
}

// Execute this statment with the given concurency.
// values_factory provides values for each execution.
// Spawns the tasks in the background and returns immediately.
fn run_queries<'a, ValFactory, Vals>(
    session: &Arc<Session>,
    statement: PreparedStatement,
    values_factory: ValFactory,
    concurrency: usize,
    name: &'static str,
) where
    ValFactory: Fn() -> Vals + Send + Sync + 'static,
    Vals: ValueList + Send,
{
    let statement = Arc::new(statement);
    let values_factory: Arc<ValFactory> = Arc::new(values_factory);

    let mut fibers = Vec::new();
    for fiber_id in 0..concurrency {
        let session = session.clone();
        let statement = statement.clone();
        let values_factory = values_factory.clone();

        fibers.push(tokio::spawn(async move {
            let report_frequency: Duration = Duration::from_millis(2000);

            let mut last_report_time: Instant = Instant::now();
            let mut queries_done: usize = 0;
            let mut latency_data = LatencyData::new();
            loop {
                let values = values_factory();
                let query_start_time = Instant::now();
                let res = session.execute(&statement, values).await;
                let query_latency: Duration = query_start_time.elapsed();
                if let Err(e) = res {
                    println!("ERROR: {}", e);
                } else {
                    queries_done += 1;
                    latency_data.add_latency(query_latency);
                }

                if fiber_id == 0 && last_report_time.elapsed() > report_frequency {
                    let queries_per_second: f64 = (queries_done * concurrency) as f64
                        / last_report_time.elapsed().as_secs_f64();
                    print!(
                        "Report[{name}, concurrency={concurrency}]: (queries/second: {queries_per_second:.2}) latencies: ",
                    );
                    latency_data.print_percentiles(&[0.95, 0.99]);
                    println!("");

                    last_report_time = Instant::now();
                    queries_done = 0;
                    latency_data.clear();
                }
            }
        }));
    }
}

pub async fn write_two_nodes_third_slower(
    nodes: &[String],
    insert_concurrency: usize,
    select_concurrency: usize,
    create_mv: bool,
    replication_factor: usize,
    total_rows: usize,
) {
    // Print configuration
    println!("Configuration:");
    println!("nodes: {:?}", nodes);
    println!("insert_concurrency: {}", insert_concurrency);
    println!("select_concurrency: {}", select_concurrency);
    println!("create_mv: {}", create_mv);
    println!("replication_factor: {}", replication_factor);
    println!("total_rows: {}", total_rows);
    println!("");

    if nodes.len() != 3 {
        panic!("Expected 3 nodes");
    }

    let fast_nodes: &[String] = &nodes[..2];
    let slow_node: &String = &nodes[2];
    println!("Fast nodes: {:?}", fast_nodes);
    println!("Slow node: {:?}", slow_node);

    let filter_nodes = fast_nodes
        .iter()
        .cloned()
        .map(|node_addr| match node_addr.to_socket_addrs() {
            Ok(_) => [node_addr.clone(), node_addr].into_iter(),
            Err(_) => [format!("{node_addr}:9042"), format!("{node_addr}:19042")].into_iter(),
        })
        .flatten();
    let host_filter = AllowListHostFilter::new(filter_nodes).unwrap();
    let session: Session = SessionBuilder::new()
        .known_nodes(fast_nodes)
        .host_filter(Arc::new(host_filter))
        .build()
        .await
        .unwrap();
    let session: Arc<Session> = Arc::new(session);

    assert_eq!(session.get_cluster_data().get_nodes_info().len(), 3);

    let ks_name: String = if create_mv {
        format!("test_ks_mv{replication_factor}")
    } else {
        format!("test_ks{replication_factor}")
    };

    let create_ks_query = format!(
        "CREATE KEYSPACE IF NOT EXISTS {ks_name} WITH replication = \
        {{'class': 'NetworkTopologyStrategy', 'replication_factor': {replication_factor}}}"
    );

    println!("{create_ks_query}");
    session.query(create_ks_query, ()).await.unwrap();
    session.use_keyspace(ks_name, true).await.unwrap();

    let create_table_query: &str =
        "CREATE TABLE IF NOT EXISTS tab (p int, c int, r int, b blob, PRIMARY KEY (p, c))";

    println!("{create_table_query}");
    session.query(create_table_query, ()).await.unwrap();

    let create_mv_query: &str =
        "CREATE MATERIALIZED VIEW IF NOT EXISTS tab_view AS SELECT p, c, r, b FROM tab \
        WHERE p IS NOT NULL and c IS NOT NULL AND r IS NOT NULL PRIMARY KEY (r, p, c)";

    if create_mv {
        println!("{create_mv_query}");
        session.query(create_mv_query, ()).await.unwrap();
    }

    let mut insert: PreparedStatement = session
        .prepare("INSERT INTO tab (p, c, r, b) VALUES (?, ?, ?, ?) USING TIMEOUT 120s")
        .await
        .unwrap();
    insert.set_consistency(Consistency::Quorum);
    insert.set_request_timeout(Some(Duration::from_secs(2 * 60)));

    let max_row: i32 = (total_rows as f64).sqrt() as i32;
    let insert_values_factory = move || {
        let p: i32 = rand::thread_rng().gen_range(0..max_row);
        let c: i32 = rand::thread_rng().gen_range(0..max_row);
        let r: i32 = rand::thread_rng().gen_range(0..max_row);
        let b: Vec<u8> = vec![1u8, 2, 3, 4];
        (p, c, r, b)
    };

    let mut select: PreparedStatement = session
        .prepare("SELECT p, c, r, b FROM tab WHERE p = ? AND c = ? BYPASS CACHE USING TIMEOUT 120s")
        .await
        .unwrap();
    select.set_consistency(Consistency::All);
    select.set_request_timeout(Some(Duration::from_secs(2 * 60)));

    let select_values_factory = move || {
        let p: i32 = rand::thread_rng().gen_range(0..max_row);
        let c: i32 = rand::thread_rng().gen_range(0..max_row);
        (p, c)
    };

    run_queries(
        &session,
        insert,
        insert_values_factory,
        insert_concurrency,
        "inserts",
    );
    run_queries(
        &session,
        select,
        select_values_factory,
        select_concurrency,
        "selects",
    );

    // Sleep for 10 years
    tokio::time::sleep(Duration::from_secs(60 * 60 * 24 * 365 * 10)).await;
}
