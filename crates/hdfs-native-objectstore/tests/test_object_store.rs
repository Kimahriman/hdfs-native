#[cfg(feature = "integration-test")]
mod common;

#[cfg(feature = "integration-test")]
mod test {
    use bytes::{Buf, BufMut, Bytes, BytesMut};
    use hdfs_native::{minidfs::DfsFeatures, Client};
    use hdfs_native_objectstore::HdfsObjectStore;
    use serial_test::serial;
    use std::collections::HashSet;

    use crate::common::{setup, TEST_FILE_INTS};

    #[tokio::test]
    #[serial]
    async fn test_object_store() -> object_store::Result<()> {
        let dfs = setup(&HashSet::from([DfsFeatures::HA]));
        let client = Client::new(&dfs.url)?;
        let store = HdfsObjectStore::new(client);

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
            store.list(Some(&Path::from("/"))).await?.collect().await;

        assert_eq!(list.len(), 1);
        assert_eq!(list[0].as_ref().unwrap().location, Path::from("/testfile"));

        // Listing of a prefix that doesn't exist should return an empty result, not an error
        assert_eq!(
            store
                .list(Some(&Path::from("/doesnt/exist")))
                .await?
                .count()
                .await,
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
        let offset = TEST_FILE_INTS as usize / 2 * 4;
        let mut buf = store.get_range(&location, offset..(offset + 4)).await?;
        assert_eq!(buf.get_i32(), TEST_FILE_INTS as i32 / 2);
        Ok(())
    }

    async fn test_object_store_write(store: &HdfsObjectStore) -> object_store::Result<()> {
        use object_store::{path::Path, ObjectStore};

        store.put(&Path::from("/newfile"), Bytes::new()).await?;
        store.head(&Path::from("/newfile")).await?;

        // Check a small files, a file that is exactly one block, and a file slightly bigger than a block
        for size_to_check in [16i32, 128 * 1024 * 1024, 130 * 1024 * 1024] {
            let ints_to_write = size_to_check / 4;

            // let mut writer = client.create("/newfile", write_options.clone()).await?;

            let mut data = BytesMut::with_capacity(size_to_check as usize);
            for i in 0..ints_to_write {
                data.put_i32(i);
            }

            let buf = data.freeze();
            store.put(&Path::from("/newfile"), buf.clone()).await?;

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
        use hdfs_native::HdfsError;
        use object_store::{path::Path, ObjectStore};
        use tokio::io::AsyncWriteExt;

        let (_, mut writer) = store.put_multipart(&"/newfile".into()).await?;
        writer.shutdown().await.map_err(HdfsError::from)?;

        store.put(&Path::from("/newfile"), Bytes::new()).await?;
        store.head(&Path::from("/newfile")).await?;

        // Check a small files, a file that is exactly one block, and a file slightly bigger than a block
        for size_to_check in [16i32, 128 * 1024 * 1024, 130 * 1024 * 1024] {
            let ints_to_write = size_to_check / 4;

            let (_, mut writer) = store.put_multipart(&"/newfile".into()).await?;

            let mut data = BytesMut::with_capacity(size_to_check as usize);
            for i in 0..ints_to_write {
                data.put_i32(i);
            }

            // Write in 10 MiB chunks
            let mut buf = data.freeze();
            while !buf.is_empty() {
                let to_write = usize::min(buf.len(), 10 * 1024 * 1024);
                writer
                    .write_all_buf(&mut buf.split_to(to_write))
                    .await
                    .map_err(HdfsError::from)?;
            }

            writer.shutdown().await.map_err(HdfsError::from)?;

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
        let (multipart_id, _) = store.put_multipart(&"/newfile".into()).await?;
        assert!(store.head(&"/.newfile.tmp".into()).await.is_ok());
        store
            .abort_multipart(&"/newfile".into(), &multipart_id)
            .await?;
        assert!(store.head(&"/.newfile.tmp".into()).await.is_err());

        Ok(())
    }

    async fn test_object_store_copy(store: &HdfsObjectStore) -> object_store::Result<()> {
        use object_store::{path::Path, ObjectStore};

        store.put(&Path::from("/newfile"), Bytes::new()).await?;

        let size_to_check = 128 * 1024 * 1024;
        let ints_to_write = size_to_check / 4;
        let mut data = BytesMut::with_capacity(size_to_check as usize);
        for i in 0..ints_to_write {
            data.put_i32(i);
        }

        let buf = data.freeze();
        store.put(&Path::from("/newfile"), buf.clone()).await?;
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
