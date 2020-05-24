extern crate odbc;
extern crate dotenv;
// Use this crate and set environmet variable RUST_LOG=odbc to see ODBC warnings
extern crate env_logger;
use odbc::*;
use odbc_safe::AutocommitOn;
use dotenv::dotenv;
use std::env;
use std::process;
use std::time::Instant;

fn main() {

    env_logger::init();

    dotenv().ok();

    let server_addr = env::var("SERVER").unwrap_or_default();
    let database = env::var("DATABASE").unwrap_or_default();
    
    let uid = env::var("UID").unwrap_or_default();    
    let pwd = env::var("PASS").unwrap_or_default();    
    

    println!("Server: {}, Database: {}, UID: {}, PWD: {}", server_addr, database, uid, pwd);

    if server_addr.is_empty() || uid.is_empty() {
        println!("Not all connection variables set!");
        process::exit(1);
    }
    match connect(server_addr, database, uid, pwd) {
        Ok(()) => println!("Success"),
        Err(diag) => println!("Error: {}", diag),
    }
}

fn connect(server_addr:String, database:String, uid:String, pwd:String) -> std::result::Result<(), DiagnosticRecord> {

    let env = create_environment_v3().map_err(|e| e.unwrap())?;    
    let buffer = String::from(format!("Driver={{ODBC Driver 17 for SQL Server}}; Server={}; Database={}; UID={}; PWD={};", server_addr, database, uid, pwd));

    let conn = env.connect_with_connection_string(&buffer)?;
    execute_statement(&conn)
}

fn execute_statement<'env>(conn: &Connection<'env, AutocommitOn>) -> Result<()> {
    let start = Instant::now();
    let stmt = Statement::with_parent(conn)?;

    let sql_text = String::from("SELECT * FROM locations order by city");
    let mut rowcount = 0;

    match stmt.exec_direct(&sql_text)? {
        Data(mut stmt) => {
            let cols = stmt.num_result_cols()?;
            while let Some(mut cursor) = stmt.fetch()? {
                rowcount += 1;
                for i in 2..(cols + 1) {
                    match cursor.get_data::<&str>(i as u16)? {
                        Some(val) => print!(" {}", val),
                        None => print!(" NULL"),
                    }
                }
                println!("");
            }
        }
        NoData(_) => println!("Query executed, no data returned"),
    }

    let duration = start.elapsed();
    println!("row count: {:?}", rowcount);
    println!("duration: {:?}", duration);

    Ok(())
}