#[test]
fn help_does_not_disclose_database_or_api_key_environment_values() {
    let output = std::process::Command::new(env!("CARGO_BIN_EXE_eventstore"))
        .args(["serve", "--help"])
        .env(
            "EVENTSTORE_DATABASE",
            "postgres://user:db-secret@localhost/events",
        )
        .env("EVENTSTORE_API_KEY", "api-secret")
        .output()
        .unwrap();
    assert!(output.status.success());
    let help = String::from_utf8(output.stdout).unwrap();
    assert!(!help.contains("db-secret"));
    assert!(!help.contains("api-secret"));
}
