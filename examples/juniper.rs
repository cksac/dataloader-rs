use async_std::task;
use async_trait::async_trait;
use dataloader::cached::Loader;
use dataloader::BatchFn;
use fake::faker::company::en::CompanyName;
use fake::faker::name::en::Name;
use fake::{Dummy, Fake, Faker};
use juniper;
use juniper::{EmptyMutation, FieldResult, Variables};
use std::collections::HashMap;

pub struct CultBatcher;

#[async_trait]
impl BatchFn<i32, Cult> for CultBatcher {
    type Error = ();

    async fn load(&self, keys: &[i32]) -> HashMap<i32, Result<Cult, Self::Error>> {
        println!("load cult by batch {:?}", keys);
        let ret = keys
            .iter()
            .map(|k| {
                let mut cult: Cult = Faker.fake();
                cult.id = k.clone();
                (k.clone(), Ok(cult))
            })
            .collect();
        ret
    }
}

#[derive(Clone)]
pub struct AppContext {
    cult_loader: Loader<i32, Cult, (), CultBatcher>,
}

impl AppContext {
    pub fn new() -> AppContext {
        AppContext {
            cult_loader: Loader::new(CultBatcher),
        }
    }
}

impl juniper::Context for AppContext {}

struct Query;

#[juniper::graphql_object(Context = AppContext)]
impl Query {
    async fn persons(_context: &AppContext) -> FieldResult<Vec<Person>> {
        let persons = fake::vec![Person; 10..20];
        Ok(persons)
    }

    async fn cult(&self, id: i32, ctx: &AppContext) -> Cult {
        ctx.cult_loader.load(id).await.expect("get cult")
    }
}

type Schema = juniper::RootNode<'static, Query, EmptyMutation<AppContext>>;

#[derive(Debug, Clone, Dummy)]
pub struct Person {
    #[dummy(faker = "1..999")]
    pub id: i32,
    #[dummy(faker = "Name()")]
    pub name: String,
    #[dummy(faker = "1..999")]
    pub cult: i32,
}

#[juniper::graphql_object(Context = AppContext)]
impl Person {
    pub fn id(&self) -> i32 {
        self.id
    }

    pub fn name(&self) -> &str {
        self.name.as_str()
    }

    pub async fn cult(&self, ctx: &AppContext) -> FieldResult<Option<Cult>> {
        let fut = ctx.cult_loader.load(self.cult);
        Ok(Some(fut.await.expect("get cult")))
    }

    pub async fn cult_by_id(&self, id: i32, ctx: &AppContext) -> Cult {
        ctx.cult_loader.load(id).await.expect("get cult")
    }
}

#[derive(Debug, Clone, Dummy)]
pub struct Cult {
    #[dummy(faker = "1..999")]
    pub id: i32,
    #[dummy(faker = "CompanyName()")]
    pub name: String,
}

#[juniper::graphql_object(Context = AppContext)]
impl Cult {
    pub fn id(&self) -> i32 {
        self.id
    }

    pub fn name(&self) -> &str {
        self.name.as_str()
    }
}

fn main() {
    let ctx = AppContext::new();
    let schema = Schema::new(Query, EmptyMutation::new());
    let vars = Variables::new();
    let q = r#"
        query {
            c1: cult(id: 1) {
              id
              name
            }
            c2: cult(id: 2) {
              id
              name
            }
            c3: cult(id: 3) {
              id
              name
            }
            persons {
              id
              name
              cult {
                id
                name
              }
              c1: cultById(id: 4) {
                id
                name
              }
              c2: cultById(id: 5) {
                id
                name
              }
              c3: cultById(id: 6) {
                id
                name
              }
            }
        }"#;
    let f = juniper::execute_async(q, None, &schema, &vars, &ctx);
    let (_res, _errors) = task::block_on(f).unwrap();
}
