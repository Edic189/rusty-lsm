use bytes::Bytes;
use rusty_lsm::engine::StorageEngine;
use tempfile::tempdir;

#[tokio::test]
async fn test_crash_recovery() {
    let dir = tempdir().unwrap();
    let path = dir.path().to_owned();

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
    }

    {
        let engine = StorageEngine::new(path.clone()).await.unwrap();

        let val1 = engine.get(b"key1").await.unwrap();
        assert_eq!(val1, Some(b"value1".to_vec()));

        let val2 = engine.get(b"key2").await.unwrap();
        assert_eq!(val2, None);
    }
}
