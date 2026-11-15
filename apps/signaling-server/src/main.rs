use signaling_server::{ServerState, router};
#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let address = std::env::var("VOICE_SIGNALING_BIND").unwrap_or_else(|_| "127.0.0.1:3000".into());
    let listener = tokio::net::TcpListener::bind(&address).await?;
    println!("Signaling listening on {address}");
    let mut state = ServerState::default();
    if let Ok(token) = std::env::var("VOICE_SIGNALING_TOKEN") {
        state = state.with_token(&token);
    }
    axum::serve(listener, router(state)).await?;
    Ok(())
}
