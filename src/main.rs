pub mod tests;
pub mod utils;

use clap::{Parser, Subcommand};

const ABOUT: &'static str =
"
rusty-scylla-tests is a collection of Scylla tests written in Rust.
Example usage:
* Print help: cargo run --release -- --help
* Run test: cargo run --release -- many-views-partitions-delete
* See test arguments: cargo run --release -- many-views-partitions-delete --help
* Run test with arguments: cargo run --release -- many-views-partitions-delete --nodes 10.0.1.3,10.0.1.4

All tests require a running scylla cluster, you can start a cluster using ccm.
";

#[derive(Parser)]
#[command(author, version, about=ABOUT)]
struct Cli {
    #[command(subcommand)]
    command: Option<Command>,
}

#[derive(Subcommand)]
enum Command {
    /// Runs the many_views_partitions_delete test
    ManyViewsPartitionsDelete {
        /// Addresses of nodes to connect to
        #[arg(long, short, num_args = 1.., value_delimiter=',', default_values_t = ["127.0.0.1".to_string()])]
        nodes: Vec<String>,

        /// Number of partitions to create
        #[arg(long, short, default_value_t = 256)]
        pk_count: usize,

        /// Number of materialized views to create
        #[arg(long, short, default_value_t = 50)]
        views_count: usize,

        /// Total number of rows that the database will store. workload_rows_number = inserted_rows * (1 + views_count)
        #[arg(long, short, default_value_t = 10_000_000)]
        workload_rows: usize,

        /// Concurrency used for inserting values
        #[arg(long, short = 'c', default_value_t = 128)]
        insert_concurrency: usize,
    },
    /// Runs the many_view_updates_to_one_node test
    ManyViewUpdatesToOneNode {
        /// Addresses of nodes to connect to
        #[arg(long, short, num_args = 1.., value_delimiter=',', default_values_t = ["127.0.0.1".to_string()])]
        nodes: Vec<String>,

        /// Number of partitions to create
        #[arg(long, short, default_value_t = 256)]
        pk_count: usize,

        /// Number of materialized views to create
        #[arg(long, short, default_value_t = 32)]
        views_count: usize,

        /// Total number of rows that the database will store. workload_rows_number = inserted_rows * (1 + views_count)
        #[arg(long, short, default_value_t = 10_000_000)]
        workload_rows: usize,

        /// Concurrency used for inserting values
        #[arg(long, short = 'c', default_value_t = 32)]
        insert_concurrency: usize,
    },
    /// Runs the write_two_nodes_third_slower test
    WriteTwoNodesThirdSlower {
        /// Addresses of nodes to connect to, should be 3 nodes, the third should be the slower one.
        #[arg(long, short, num_args = 1.., value_delimiter=',', default_values_t = ["127.0.0.1".to_string(), "127.0.0.2".to_string(), "127.0.0.3".to_string()])]
        nodes: Vec<String>,

        /// Concurrency used for inserting values
        #[arg(long, short, default_value_t = 128)]
        insert_concurrency: usize,

        /// Concurrency used for selecting values
        #[arg(long, short, default_value_t = 4)]
        select_concurrency: usize,

        /// Replication factor to use
        #[arg(long, short, default_value_t = 3)]
        replication_factor: usize,

        /// Create a materialized view before starting the workload
        #[arg(long, short, default_value_t = false)]
        create_mv: bool,

        /// Total number of rows that will be inserted
        #[arg(long, short, default_value_t = 10_000_000_000)]
        total_rows: usize,
    },
}

#[tokio::main]
async fn main() {
    let cli = Cli::parse();

    let command = match cli.command {
        Some(command) => command,
        None => {
            println!("{}", ABOUT);
            return;
        }
    };

    match command {
        Command::ManyViewsPartitionsDelete {
            nodes,
            pk_count,
            views_count,
            workload_rows,
            insert_concurrency,
        } => {
            tests::many_views_partitions_delete::many_views_partitions_delete(
                &nodes,
                pk_count,
                views_count,
                workload_rows,
                insert_concurrency,
            )
            .await;
        }
        Command::ManyViewUpdatesToOneNode {
            nodes,
            pk_count,
            views_count,
            workload_rows,
            insert_concurrency,
        } => {
            tests::many_view_updates_to_one_node::many_view_updates_to_one_node(
                &nodes,
                pk_count,
                views_count,
                workload_rows,
                insert_concurrency,
            )
            .await;
        }
        Command::WriteTwoNodesThirdSlower {
            nodes,
            insert_concurrency,
            select_concurrency,
            create_mv,
            replication_factor,
            total_rows,
        } => {
            tests::write_two_nodes_third_slower::write_two_nodes_third_slower(
                &nodes,
                insert_concurrency,
                select_concurrency,
                create_mv,
                replication_factor,
                total_rows,
            )
            .await;
        }
    }
}
