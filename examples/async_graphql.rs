use async_graphql::{Context, EmptyMutation, EmptySubscription, Schema};
use async_trait::async_trait;
use dataloader::cached::Loader;
use dataloader::BatchFn;
use fake::faker::company::en::CompanyName;
use fake::faker::name::en::Name;
use fake::{Dummy, Fake, Faker};
use futures::executor::block_on;
use std::collections::HashMap;

pub struct CultBatcher;

#[async_trait]
impl BatchFn<i32, Cult> for CultBatcher {
    async fn load(&self, keys: &[i32]) -> HashMap<i32, Cult> {
        println!("load cult by batch {:?}", keys);
        let ret = keys
            .iter()
            .map(|k| {
                let mut cult: Cult = Faker.fake();
                cult.id = k.clone();
                (k.clone(), cult)
            })
            .collect();
        ret
    }
}

#[derive(Clone)]
pub struct AppContext {
    cult_loader: Loader<i32, Cult, CultBatcher>,
}

impl AppContext {
    pub fn new() -> AppContext {
        AppContext {
            cult_loader: Loader::new(CultBatcher),
        }
    }
}

struct Query;

#[async_graphql::Object]
impl Query {
    async fn persons(&self, _ctx: &Context<'_>) -> Vec<Person> {
        let persons = fake::vec![Person; 10..20];
        persons
    }

    async fn cult(&self, ctx: &Context<'_>, id: i32) -> Cult {
        ctx.data_unchecked::<AppContext>()
            .cult_loader
            .load(id)
            .await
    }
}

#[derive(Debug, Clone, Dummy)]
pub struct Person {
    #[dummy(faker = "1..999")]
    pub id: i32,
    #[dummy(faker = "Name()")]
    pub name: String,
    #[dummy(faker = "1..999")]
    pub cult: i32,
}

#[async_graphql::Object]
impl Person {
    async fn id(&self) -> i32 {
        self.id
    }

    async fn name(&self) -> &str {
        self.name.as_str()
    }

    async fn cult(&self, ctx: &Context<'_>) -> Cult {
        ctx.data_unchecked::<AppContext>()
            .cult_loader
            .load(self.cult)
            .await
    }

    async fn cult_by_id(&self, ctx: &Context<'_>, id: i32) -> Cult {
        ctx.data_unchecked::<AppContext>()
            .cult_loader
            .load(id)
            .await
    }
}

#[derive(Debug, Clone, Dummy)]
pub struct Cult {
    #[dummy(faker = "1..999")]
    pub id: i32,
    #[dummy(faker = "CompanyName()")]
    pub name: String,
}

#[async_graphql::Object]
impl Cult {
    async fn id(&self) -> i32 {
        self.id
    }

    async fn name(&self) -> &str {
        self.name.as_str()
    }
}

fn main() {
    let schema = Schema::build(Query, EmptyMutation, EmptySubscription)
        .data(AppContext::new())
        .finish();
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
    let f = schema.execute(q);
    let _r = block_on(f);
}
