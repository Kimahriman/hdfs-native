#[cfg(feature = "integration-test")]
mod test {
    use bytes::{Buf, BufMut, BytesMut};
    use hdfs_native::{
        minidfs::{DfsFeatures, MiniDfs},
        Client, WriteOptions,
    };
    use hdfs_native_object_store::{HdfsErrorConvert, HdfsObjectStore};
    use object_store::{PutMode, PutOptions, PutPayload};
    use serial_test::serial;
    use std::{collections::HashSet, sync::Arc};

    pub const TEST_FILE_INTS: usize = 64 * 1024 * 1024;

    #[tokio::test]
    #[serial]
    async fn test_object_store() -> object_store::Result<()> {
        let dfs = MiniDfs::with_features(&HashSet::from([DfsFeatures::HA]));
        let client = Client::new(&dfs.url).to_object_store_err()?;

        // Create a test file with the client directly to sanity check reads and lists
        let mut file = client
            .create("/testfile", WriteOptions::default())
            .await
            .unwrap();
        let mut buf = BytesMut::new();
        for i in 0..TEST_FILE_INTS as i32 {
            buf.put_i32(i);
        }
        file.write(buf.freeze()).await.unwrap();
        file.close().await.unwrap();

        let store = HdfsObjectStore::new(Arc::new(client));

        test_object_store_head(&store).await?;
        test_object_store_list(&store).await?;
        test_object_store_rename(&store).await?;
        test_object_store_read(&store).await?;
        test_object_store_write(&store).await?;
        test_object_store_write_multipart(&store).await?;
        test_object_store_copy(&store).await?;

        Ok(())
    }

    async fn test_object_store_head(store: &HdfsObjectStore) -> object_store::Result<()> {
        use object_store::{path::Path, ObjectStore};

        let head = store.head(&Path::from("/testfile")).await?;
        assert_eq!(head.location, Path::from("/testfile"));
        assert_eq!(head.size, TEST_FILE_INTS * 4);

        assert!(store.head(&Path::from("/testfile2")).await.is_err());

        Ok(())
    }

    async fn test_object_store_list(store: &HdfsObjectStore) -> object_store::Result<()> {
        use futures::StreamExt;
        use object_store::{path::Path, ObjectMeta, ObjectStore};

        let list: Vec<object_store::Result<ObjectMeta>> =
            store.list(Some(&Path::from("/"))).collect().await;

        assert_eq!(list.len(), 1);
        assert_eq!(list[0].as_ref().unwrap().location, Path::from("/testfile"));

        // Listing of a prefix that doesn't exist should return an empty result, not an error
        assert_eq!(
            store.list(Some(&Path::from("/doesnt/exist"))).count().await,
            0
        );

        let list = store
            .list_with_delimiter(Some(&Path::from("/doesnt/exist")))
            .await?;

        assert_eq!(list.common_prefixes.len(), 0);
        assert_eq!(list.objects.len(), 0);

        Ok(())
    }

    async fn test_object_store_rename(store: &HdfsObjectStore) -> object_store::Result<()> {
        use object_store::{path::Path, ObjectStore};

        store
            .rename(&Path::from("/testfile"), &Path::from("/testfile2"))
            .await?;

        assert!(store.head(&Path::from("/testfile2")).await.is_ok());
        assert!(store.head(&Path::from("/testfile")).await.is_err());

        store
            .rename(&Path::from("/testfile2"), &Path::from("/testfile"))
            .await?;

        assert!(store.head(&Path::from("/testfile")).await.is_ok());
        assert!(store.head(&Path::from("/testfile2")).await.is_err());

        Ok(())
    }

    async fn test_object_store_read(store: &HdfsObjectStore) -> object_store::Result<()> {
        use object_store::{path::Path, ObjectStore};

        let location = Path::from("/testfile");

        let mut buf = store.get(&location).await?.bytes().await?;
        for i in 0..TEST_FILE_INTS as i32 {
            assert_eq!(buf.get_i32(), i);
        }

        // Read a single integer from the file
        let offset = TEST_FILE_INTS / 2 * 4;
        let mut buf = store.get_range(&location, offset..(offset + 4)).await?;
        assert_eq!(buf.get_i32(), TEST_FILE_INTS as i32 / 2);
        Ok(())
    }

    async fn test_object_store_write(store: &HdfsObjectStore) -> object_store::Result<()> {
        use object_store::{path::Path, ObjectStore};

        store
            .put(&Path::from("/newfile"), PutPayload::new())
            .await?;
        store.head(&Path::from("/newfile")).await?;

        // PutMode = Create should fail for existing file
        match store
            .put_opts(
                &Path::from("/newfile"),
                PutPayload::new(),
                PutOptions {
                    mode: PutMode::Create,
                    ..Default::default()
                },
            )
            .await
        {
            Err(object_store::Error::AlreadyExists { .. }) => (),
            Err(e) => panic!("Wrong error was thrown for put without overewrite: {:?}", e),
            Ok(_) => panic!("No error was thrown for put without overwrite for existing file"),
        }

        // Check a small files, a file that is exactly one block, and a file slightly bigger than a block
        for size_to_check in [16i32, 128 * 1024 * 1024, 130 * 1024 * 1024] {
            let ints_to_write = size_to_check / 4;

            let mut data = BytesMut::with_capacity(size_to_check as usize);
            for i in 0..ints_to_write {
                data.put_i32(i);
            }

            let buf = data.freeze();
            store
                .put(&Path::from("/newfile"), PutPayload::from_bytes(buf.clone()))
                .await?;

            assert_eq!(
                store.head(&Path::from("/newfile")).await?.size,
                size_to_check as usize
            );

            let read_data = store.get(&Path::from("/newfile")).await?.bytes().await?;

            assert_eq!(buf.len(), read_data.len());

            for pos in 0..buf.len() {
                assert_eq!(
                    buf[pos], read_data[pos],
                    "data is different as position {} for size {}",
                    pos, size_to_check
                );
            }
        }

        store.delete(&Path::from("/newfile")).await?;

        Ok(())
    }

    async fn test_object_store_write_multipart(
        store: &HdfsObjectStore,
    ) -> object_store::Result<()> {
        use object_store::{path::Path, ObjectStore};

        let mut uploader = store.put_multipart(&"/newfile".into()).await?;
        uploader.complete().await?;

        store.head(&Path::from("/newfile")).await?;

        // Check a small files, a file that is exactly one block, and a file slightly bigger than a block
        for size_to_check in [16i32, 128 * 1024 * 1024, 130 * 1024 * 1024] {
            let ints_to_write = size_to_check / 4;

            let mut uploader = store.put_multipart(&"/newfile".into()).await?;

            let mut data = BytesMut::with_capacity(size_to_check as usize);
            for i in 0..ints_to_write {
                data.put_i32(i);
            }

            // Write in 10 MiB chunks
            let mut buf = data.freeze();
            while !buf.is_empty() {
                let to_write = usize::min(buf.len(), 10 * 1024 * 1024);
                uploader.put_part(buf.split_to(to_write).into()).await?;
            }

            uploader.complete().await?;

            assert_eq!(
                store.head(&Path::from("/newfile")).await?.size,
                size_to_check as usize
            );

            let mut read_data = store.get(&Path::from("/newfile")).await?.bytes().await?;

            assert_eq!(size_to_check as usize, read_data.len());

            for pos in 0..ints_to_write {
                assert_eq!(
                    pos,
                    read_data.get_i32(),
                    "data is different at integer {} for size {}",
                    pos,
                    size_to_check
                );
            }
        }

        store.delete(&Path::from("/newfile")).await?;

        // Test aborting
        let mut uploader = store.put_multipart(&"/newfile".into()).await?;
        assert!(store.head(&"/.newfile.tmp.1".into()).await.is_ok());
        uploader.abort().await?;
        assert!(store.head(&"/.newfile.tmp.1".into()).await.is_err());
        assert!(store.head(&"/newfile".into()).await.is_err());

        // Test multiple uploads to the same destination, default is to overwrite
        let mut uploader1 = store.put_multipart(&"/newfile".into()).await?;
        let mut uploader2 = store.put_multipart(&"/newfile".into()).await?;
        uploader1.put_part(vec![1].into()).await?;
        uploader2.put_part(vec![2].into()).await?;
        uploader1.complete().await?;
        uploader2.complete().await?;

        let result = store.get(&"/newfile".into()).await?;
        assert!(result.bytes().await?.to_vec() == vec![2]);

        Ok(())
    }

    async fn test_object_store_copy(store: &HdfsObjectStore) -> object_store::Result<()> {
        use object_store::{path::Path, ObjectStore};

        store
            .put(&Path::from("/newfile"), PutPayload::new())
            .await?;

        let size_to_check = 128 * 1024 * 1024;
        let ints_to_write = size_to_check / 4;
        let mut data = BytesMut::with_capacity(size_to_check as usize);
        for i in 0..ints_to_write {
            data.put_i32(i);
        }

        let buf = data.freeze();
        store
            .put(&Path::from("/newfile"), buf.clone().into())
            .await?;
        store
            .copy(&Path::from("/newfile"), &Path::from("/newfile2"))
            .await?;

        let read_data = store.get(&Path::from("/newfile2")).await?.bytes().await?;
        assert_eq!(buf.len(), read_data.len());

        for pos in 0..buf.len() {
            assert_eq!(
                buf[pos], read_data[pos],
                "data is different as position {} for size {}",
                pos, size_to_check
            );
        }

        assert!(store
            .copy_if_not_exists(&Path::from("/newfile"), &Path::from("/newfile2"))
            .await
            .is_err());

        store.delete(&Path::from("/newfile")).await?;
        store.delete(&Path::from("/newfile2")).await?;

        Ok(())
    }
}
