use datafusion::dataframe::DataFrameWriteOptions;
use datafusion::prelude::SessionContext;
use std::sync::Arc;
use crate::datasources::HttpDataSource;

pub async fn run() -> datafusion::error::Result<()> {
    // Create session context
    let ctx = SessionContext::new();
    
    // Create HTTP data source
    let db = HttpDataSource::default();
    
    // Populate data from API
    println!("📡 Fetching data from API...");
    db.populate_data(
        String::from("https://jsonplaceholder.typicode.com/posts"), 
        None, 
        String::from("GET")
    ).await?;
    
    // Register the data source as a table
    ctx.register_table("posts", Arc::new(db))?;
    println!("✅ Data source registered as 'posts' table");
    
    // Example 1: Simple SELECT query
    println!("\n🔍 Example 1: Simple SELECT");
    let df1 = ctx.sql("SELECT * FROM posts LIMIT 5").await?;
    df1.show().await?;
    
    // Example 2: Count records
    println!("\n📊 Example 2: Count all records");
    let df2 = ctx.sql("SELECT COUNT(*) as total_posts FROM posts").await?;
    df2.show().await?;
    
    // Example 3: Filter and group (with proper case)
    println!("\n🔍 Example 3: Group by userId");
    let df3 = ctx.sql(
        "SELECT \"userId\", COUNT(*) as post_count 
         FROM posts 
         GROUP BY \"userId\" 
         ORDER BY post_count DESC"
    ).await?;
    df3.show().await?;
    
    // Example 4: Filter specific user (with proper case)
    println!("\n👤 Example 4: Posts from user 1");
    let df4 = ctx.sql(
        "SELECT \"id\", \"title\", \"body\" 
         FROM posts 
         WHERE \"userId\" = 1"
    ).await?;
    df4.show().await?;
    
    // Example 5: Search in title (with proper case)
    println!("\n🔍 Example 5: Search titles containing 'qui'");
    let df5 = ctx.sql(
        "SELECT \"id\", \"userId\", \"title\" 
         FROM posts 
         WHERE \"title\" LIKE '%qui%'"
    ).await?;
    df5.show().await?;
    
    // Interactive query execution
    println!("\n🎯 You can now execute custom queries!");
    execute_interactive_queries(&ctx).await?;
    
    // Save results to file
    println!("\n💾 Saving all data to CSV...");
    let all_data = ctx.sql("SELECT * FROM posts").await?;
    all_data.write_csv("posts_output.csv", DataFrameWriteOptions::default(), None).await?;
    println!("✅ Data saved to posts_output.csv");

    execute_specific_query().await?;
    
    Ok(())
}

async fn execute_interactive_queries(ctx: &SessionContext) -> datafusion::error::Result<()> {
    use std::io::{self, Write};
    
    println!("Enter SQL queries (type 'exit' to quit, 'help' for examples):");
    
    loop {
        print!("sql> ");
        io::stdout().flush().unwrap();
        
        let mut input = String::new();
        io::stdin().read_line(&mut input).unwrap();
        let query = input.trim();
        
        match query.to_lowercase().as_str() {
            "exit" | "quit" => {
                println!("👋 Goodbye!");
                break;
            },
            "help" => {
                print_help();
                continue;
            },
            "" => continue,
            _ => {
                match ctx.sql(query).await {
                    Ok(df) => {
                        match df.show().await {
                            Ok(_) => {},
                            Err(e) => println!("❌ Error displaying results: {}", e),
                        }
                    },
                    Err(e) => println!("❌ Query error: {}", e),
                }
            }
        }
    }
    
    Ok(())
}

fn print_help() {
    println!("\n📖 Example queries you can try:");
    println!("   SELECT * FROM posts LIMIT 10");
    println!("   SELECT \"userId\", COUNT(*) FROM posts GROUP BY \"userId\"");
    println!("   SELECT * FROM posts WHERE \"userId\" = 2");
    println!("   SELECT \"id\", \"title\" FROM posts WHERE \"title\" LIKE '%dolor%'");
    println!("   SELECT AVG(LENGTH(\"body\")) as avg_body_length FROM posts");
    println!("   SELECT \"userId\", MAX(\"id\") as latest_post FROM posts GROUP BY \"userId\"");
    println!("\n💡 Available columns: \"id\", \"userId\", \"title\", \"body\"");
    println!("   Note: Use double quotes around column names due to case sensitivity");
    println!("   Type 'exit' to quit\n");
}

// Alternative main function for specific query execution
#[allow(dead_code)]
async fn execute_specific_query() -> datafusion::error::Result<()> {
    let ctx = SessionContext::new();
    let db = HttpDataSource::default();
    
    // Fetch data
    db.populate_data(
        String::from("https://jsonplaceholder.typicode.com/posts"), 
        None, 
        String::from("GET")
    ).await?;
    
    // Register table
    ctx.register_table("posts", Arc::new(db))?;
    
    // Execute your specific query here
    let query = "
        SELECT 
            \"userId\",
            COUNT(*) as total_posts,
            AVG(LENGTH(\"title\")) as avg_title_length,
            AVG(LENGTH(\"body\")) as avg_body_length
        FROM posts 
        GROUP BY \"userId\" 
        ORDER BY total_posts DESC
    ";
    
    println!("🚀 Executing query:");
    println!("{}", query);
    
    let df = ctx.sql(query).await?;
    df.clone().show().await?;
    
    // Save to CSV
    df.write_csv("analysis_output.csv", DataFrameWriteOptions::default(), None).await?;
    println!("💾 Results saved to analysis_output.csv");
    
    Ok(())
}

// Function to work with different APIs
#[allow(dead_code)]
async fn query_different_api() -> datafusion::error::Result<()> {
    let ctx = SessionContext::new();
    
    // Example with different API endpoints
    let apis = vec![
        ("users", "https://jsonplaceholder.typicode.com/users"),
        ("posts", "https://jsonplaceholder.typicode.com/posts"),
        ("comments", "https://jsonplaceholder.typicode.com/comments"),
    ];
    
    // Register multiple data sources
    for (table_name, url) in apis {
        let db = HttpDataSource::default();
        db.populate_data(String::from(url), None, String::from("GET")).await?;
        ctx.register_table(table_name, Arc::new(db))?;
        println!("✅ Registered table: {}", table_name);
    }
    
    // Now you can join across different APIs
    let join_query = "
        SELECT 
            u.\"name\" as user_name,
            u.\"email\",
            COUNT(p.\"id\") as post_count
        FROM users u
        LEFT JOIN posts p ON u.\"id\" = p.\"userId\"
        GROUP BY u.\"id\", u.\"name\", u.\"email\"
        ORDER BY post_count DESC
    ";
    
    println!("🔗 Executing join query across APIs:");
    let df = ctx.sql(join_query).await?;
    df.show().await?;
    
    Ok(())
}