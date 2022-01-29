use std::iter::Successors;
use std::os::linux::raw::stat;
use std::process::exit;
use std::ptr::null;

use anyhow::{anyhow, Result};
use rustyline::Editor;
use rustyline::error::ReadlineError;
use crate::StatementType::STATEMENT_NONE;

#[tokio::main]
async fn main() -> Result<()> {
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
                let stat = Statement::default();
                match (prepare_statment(&line, stat)) {
                    PrepareResult::PREPARE_SUCCESS => {}
                    PrepareResult::PREPARE_UNRECOGNIZED_STATEMENT => {}
                }
                execute_statement(&stat)
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

enum MetaCommandResult {
    META_COMMAND_SUCCESS,
    META_COMMAND_UNRECOGNIZED_COMMAND,
}

enum PrepareResult {
    PREPARE_SUCCESS,
    PREPARE_UNRECOGNIZED_STATEMENT,
}

enum StatementType {
    STATEMENT_NONE,
    STATEMENT_INSERT,
    STATEMENT_SELECT,
}

fn do_meta_command(input: &str) -> MetaCommandResult {
    if (input.eq(".exit")) {
        MetaCommandResult::META_COMMAND_SUCCESS
    } else {
        MetaCommandResult::META_COMMAND_UNRECOGNIZED_COMMAND
    }
}

fn prepare_statment(input: &str, mut stat: Statement) -> PrepareResult {
    if ("insert".eq(&input[0..12])) {
        stat.statmentType = StatementType::STATEMENT_INSERT;
        return PrepareResult::PREPARE_SUCCESS
    }
    if ("select".eq(&input[0..12])) {
        stat.statmentType = StatementType::STATEMENT_SELECT;
        return PrepareResult::PREPARE_SUCCESS
    }
    return PrepareResult::PREPARE_UNRECOGNIZED_STATEMENT
}

pub struct Statement {
    pub statmentType : StatementType,
}

impl Default for Statement {
    fn default() -> Self {
        Self::new()
    }
}

impl Statement {
    pub fn new() -> Self {
        Statement {
            statmentType:STATEMENT_NONE,
        }
    }
}

fn execute_statement(stat: &Statement) {
    match stat.statmentType {
        StatementType::STATEMENT_INSERT => {
            println!("insert");
        }
        StatementType::STATEMENT_SELECT => {
            println!("select");
        }
        _ => {}
    }
}