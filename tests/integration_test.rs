// tests/integration_test.rs
use bytes::Bytes;
use rusty_lsm::engine::StorageEngine;
use tempfile::tempdir;

#[tokio::test]
async fn test_crash_recovery() {
    let dir = tempdir().unwrap();
    let path = dir.path().to_owned();

    // 1. Start Engine, Write Data
    {
        let engine = StorageEngine::new(path.clone()).await.unwrap();
        engine
            .put(Bytes::from("key1"), Bytes::from("value1"))
            .await
            .unwrap();
        engine
            .put(Bytes::from("key2"), Bytes::from("value2"))
            .await
            .unwrap();
        engine.delete(Bytes::from("key2")).await.unwrap();
    } // Engine drops here

    // 2. Restart Engine (Recovery)
    {
        let engine = StorageEngine::new(path.clone()).await.unwrap();

        // Check key1 (Should exist via WAL)
        let val1 = engine.get(b"key1").await.unwrap();
        assert_eq!(val1, Some(b"value1".to_vec()));

        // Check key2 (Should be tombstoned via WAL)
        let val2 = engine.get(b"key2").await.unwrap();
        assert_eq!(val2, None);
    }
}
