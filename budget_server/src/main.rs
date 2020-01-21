use data::{Database, Disk, Key};
use std::convert::TryInto;
use std::io;
use std::path::Path;
use std::sync::{Arc, Mutex};
use uuid::Uuid;

struct State<D: Disk> {
    db: Arc<Mutex<Database<D>>>,
}

#[derive(serde::Serialize, serde::Deserialize)]
struct Expense {
    uuid: Uuid,
    /// the number of cents in the expense
    amount: i64,
}
impl Expense {
    const UUID: u128 = 0;
    const AMOUNT: u128 = 1;
}
struct Tables;
impl Tables {
    const EXPENSES: u128 = 0;
}
#[derive(serde::Deserialize)]
struct CreateExpenseDto {
    amount: i64,
}

fn read_expense(db: &mut Database<impl Disk>, uuid: Uuid) -> io::Result<Option<Expense>> {
    let amount_bytes = match db
        .lookup()?
        .get(Tables::EXPENSES)?
        .get(uuid.as_u128())?
        .value(Expense::AMOUNT)? {
        Some(val) => val,
        None => return Ok(None)
    };
    let amount = i64::from_be_bytes(amount_bytes.as_slice().try_into().unwrap());
    Ok(Some(Expense { uuid, amount }))
}

fn insert_expense(db: &mut Database<impl Disk>, dto: CreateExpenseDto) -> io::Result<Expense> {
    let uuid = Uuid::new_v4();
    let key = uuid.as_u128();

    db.lookup()?
        .get(Tables::EXPENSES)?
        .get(key)?
        .set_value(Expense::UUID, &key.to_be_bytes())?;
    db.lookup()?
        .get(Tables::EXPENSES)?
        .get(key)?
        .set_value(Expense::AMOUNT, &dto.amount.to_be_bytes())?;
    Ok(read_expense(db, uuid)?.unwrap())
}

async fn post_expense(mut req: tide::Request<State<impl Disk>>) -> tide::Response {
    let expense: CreateExpenseDto = req.body_json().await.unwrap();
    let mut db = req.state().db.lock().unwrap();
    match insert_expense(&mut db, expense) {
        Ok(resp) => tide::Response::new(200).body_json(&resp).unwrap(),
        Err(_) => tide::Response::new(500).body_string("Error writing expense!".to_string()),
    }
}

async fn get_expense(req: tide::Request<State<impl Disk>>) -> tide::Response {
    let uuid: Uuid = match req.param("uuid") {
        Ok(val) => val,
        Err(err) => return tide::Response::new(400).body_string(err.to_string())
    };

    let mut db = req.state().db.lock().unwrap();

    match read_expense(&mut db, uuid) {
        Ok(Some(resp)) => tide::Response::new(200).body_json(&resp).unwrap(),
        Ok(None) => tide::Response::new(404).body_string("Expense not found".to_string()),
        Err(_) => tide::Response::new(500).body_string("Error writing expense!".to_string()),
    }
}

fn get_all_expenses(db: &mut Database<impl Disk>, query: AllExpensesQuery) -> io::Result<Vec<Expense>> {
    let mut expenses = vec![];
    let all_keys = db.lookup()?.get(Tables::EXPENSES)?.keys()?.collect::<io::Result<Vec<Key>>>()?;

    for key in all_keys {
        let expense = read_expense(db, Uuid::from_u128(key))?.unwrap();
        if let Some(constraint) = query.amount_gte {
            if expense.amount < constraint {
                continue
            }
        }
        expenses.push(expense);
    }
    Ok(expenses)
}
#[derive(serde::Deserialize, Default, Debug)]
struct AllExpensesQuery {
    #[serde(rename = "amount[gte]")]
    amount_gte: Option<i64>
}

async fn all_expenses(req: tide::Request<State<impl Disk>>) -> tide::Response {
    let query: AllExpensesQuery = req.query().unwrap_or_default();
    println!("{:?}", query);

    let mut db = req.state().db.lock().unwrap();

    match get_all_expenses(&mut db, query) {
        Ok(resp) => tide::Response::new(200).body_json(&resp).unwrap(),
        Err(_) => tide::Response::new(500).body_string("Error reading expense!".to_string()),
    }
}


#[async_std::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    env_logger::init();
    let db_path = Path::new("database.dat");
    let db = if db_path.exists() {
        let file = std::fs::OpenOptions::new()
            .create(true)
            .read(true)
            .write(true)
            .open(db_path)?;
        Database::from_existing(file)?
    } else {
        let file = std::fs::OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .open(db_path)?;
        Database::initialize(file)?
    };
    let state = State {
        db: Arc::new(Mutex::new(db)),
    };

    let mut app = tide::with_state(state);

    app.at("/expenses").post(post_expense);
    app.at("/expenses/:uuid").get(get_expense);
    app.at("/expenses").get(all_expenses);
    app.listen("127.0.0.1:8080").await?;
    Ok(())
}
