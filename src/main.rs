use std::fs;
use std::fs::{File, remove_file};
use std::io::{BufReader, BufWriter, Read, Write};
use std::iter::Successors;
use std::os::linux::raw::stat;
use std::path::Path;
use std::process::exit;
use std::ptr::null;

use anyhow::{anyhow, Result};
use bincode::{config, deserialize_from, Deserializer, serialize_into};
use rustyline::Editor;
use rustyline::error::ReadlineError;
use serde::Deserialize;
use serde::Serialize;

use rdb_btree::node_type::KeyValuePair;
use rdb_btree::btree::{BTree, BTreeBuilder};
use rdb_btree::error::Error;

use crate::ExecuteResult::{EXECUTE_SUCCESS, EXECUTE_TABLE_FULL};
use crate::StatementType::STATEMENT_NONE;

const ID_OFFSET: u32 = 0;
const USERNAME_OFFSET: u32 = ID_OFFSET + 4;
const EMAIL_OFFSET: u32 = USERNAME_OFFSET + 32;
const ROW_SIZE: u32 = 291;

const PAGE_SIZE: u32 = 4096;
const ROWS_PER_PAGE: u32 = PAGE_SIZE / ROW_SIZE;
const TABLE_MAX_ROWS: u32 = ROWS_PER_PAGE * 100;

#[tokio::main]
async fn main() -> Result<()> {
    let mut btree = BTreeBuilder::new()
        .path(Path::new("../db"))
        .b_parameter(2)
        .build().unwrap();
    let mut rl = Editor::<()>::new();
    loop {
        let readline = rl.readline("db > ");
        match readline {
            Ok(line) => {
                rl.add_history_entry(line.as_str());
                if (line.starts_with(".")) {
                    match do_meta_command(&line) {
                        MetaCommandResult::META_COMMAND_SUCCESS => {}
                        MetaCommandResult::META_COMMAND_UNRECOGNIZED_COMMAND => {
                            println!("Unrecognized command");
                            continue;
                        }
                    }
                }
                let mut stat = Statement::default();
                match (prepare_statment(&line, &mut stat)) {
                    PrepareResult::PREPARE_SUCCESS => {}
                    PrepareResult::PREPARE_UNRECOGNIZED_STATEMENT => {}
                }
                execute_statement(stat, &mut btree);
            }
            Err(ReadlineError::Interrupted) => {
                println!("Interrupted");
            }
            Err(ReadlineError::Eof) => {
                println!("Exited");
                break;
            }
            _ => {}
        }
    }
    Ok(())
}

pub enum MetaCommandResult {
    META_COMMAND_SUCCESS,
    META_COMMAND_UNRECOGNIZED_COMMAND,
}

pub enum PrepareResult {
    PREPARE_SUCCESS,
    PREPARE_UNRECOGNIZED_STATEMENT,
}

pub enum StatementType {
    STATEMENT_NONE,
    STATEMENT_INSERT,
    STATEMENT_SELECT,
}

impl Default for StatementType {
    fn default() -> Self { StatementType::STATEMENT_NONE }
}

pub enum ExecuteResult {
    EXECUTE_SUCCESS,
    EXECUTE_TABLE_FULL,
}

fn do_meta_command(input: &str) -> MetaCommandResult {
    if (input.eq(".exit")) {
        //db_close(table);
        exit(0);
    } else {
        return MetaCommandResult::META_COMMAND_SUCCESS;
    }
}

fn prepare_statment(input: &str, stat: &mut Statement) -> PrepareResult {
    if ("insert".eq(&input[0..6])) {
        stat.statmentType = StatementType::STATEMENT_INSERT;
        let x: Vec<&str> = input.split(' ').collect();
        stat.row_to_insert.id = x[1].parse().unwrap();
        stat.row_to_insert.username = x[2].parse().unwrap();
        stat.row_to_insert.email = x[3].parse().unwrap();
        return PrepareResult::PREPARE_SUCCESS;
    }
    if ("select".eq(&input[0..6])) {
        stat.statmentType = StatementType::STATEMENT_SELECT;
        let x: Vec<&str> = input.split(' ').collect();
        stat.row_to_insert.id = x[1].parse().unwrap();
        return PrepareResult::PREPARE_SUCCESS;
    }
    return PrepareResult::PREPARE_UNRECOGNIZED_STATEMENT;
}

#[derive(Default)]
pub struct Statement {
    pub statmentType: StatementType,
    pub row_to_insert: Row,
}

#[derive(Default, Debug, Serialize, Deserialize)]
pub struct Row {
    pub id: u32,
    pub username: String,
    pub email: String,
}

#[derive(Default, Debug, Serialize, Deserialize)]
pub struct Table {
    pub num_rows: u32,
    pub schema_vec: Vec<Row>,
}

fn execute_statement(stat: Statement, btree: &mut BTree) {
    match stat.statmentType {
        StatementType::STATEMENT_INSERT => {
            let result = execute_insert(stat, btree);
            println!("{}", result.is_err());
        }
        StatementType::STATEMENT_SELECT => {
            let result = execute_select(stat, btree);
            println!("{}", result.is_err());
        }
        _ => {}
    }
}

fn execute_insert(stat: Statement, btree: &mut BTree) -> Result<(), Error> {
    btree.insert(KeyValuePair::new(stat.row_to_insert.id.to_string(), serde_json::to_string(&stat.row_to_insert).expect("Couldn't serialize config")))?;
    Ok(())
}

fn execute_select(stat: Statement, btree: &mut BTree) -> Result<(), Error> {
    println!("email:{}", btree.search(stat.row_to_insert.id.to_string())?.value);
    Ok(())
}


