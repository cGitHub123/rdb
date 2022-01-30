use std::iter::Successors;
use std::os::linux::raw::stat;
use std::process::exit;
use std::ptr::null;

use anyhow::{anyhow, Result};
use rustyline::Editor;
use rustyline::error::ReadlineError;

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
    let mut table = Table::default();
    let mut rl = Editor::<()>::new();
    loop {
        let readline = rl.readline("db > ");
        match readline {
            Ok(line) => {
                rl.add_history_entry(line.as_str());
                if (line.starts_with(".")) {
                    match (do_meta_command(&line)) {
                        MetaCommandResult::META_COMMAND_SUCCESS => {
                            continue;
                        }
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
                execute_statement(stat, &mut table);
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
        MetaCommandResult::META_COMMAND_SUCCESS
    } else {
        MetaCommandResult::META_COMMAND_UNRECOGNIZED_COMMAND
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
        return PrepareResult::PREPARE_SUCCESS;
    }
    return PrepareResult::PREPARE_UNRECOGNIZED_STATEMENT;
}

#[derive(Default)]
pub struct Statement {
    pub statmentType: StatementType,
    pub row_to_insert: Row,
}

#[derive(Default)]
pub struct Row {
    pub id: String,
    pub username: String,
    pub email: String,
}

#[derive(Default)]
pub struct Table {
    pub num_rows: u32,
    pub schema_vec: Vec<Row>,
}

fn execute_statement(stat: Statement, table: &mut Table) {
    match stat.statmentType {
        StatementType::STATEMENT_INSERT => {
            execute_insert(stat, table);
        }
        StatementType::STATEMENT_SELECT => {
            execute_select(stat, table);
        }
        _ => {}
    }
}

fn execute_insert(stat: Statement, table: &mut Table) -> ExecuteResult {
    if (table.num_rows >= TABLE_MAX_ROWS) {
        return EXECUTE_TABLE_FULL;
    }
    table.schema_vec.push(stat.row_to_insert);
    table.num_rows += 1;
    return EXECUTE_SUCCESS;
}

fn execute_select(stat: Statement, table: &mut Table) {
    for i in &table.schema_vec {
        println!("email:{}", i.email);
        println!("username:{}", i.username);
        println!("id:{}", i.id);
    }
}

