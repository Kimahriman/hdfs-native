use bytes::{BufMut, BytesMut};
use hdfs_native::client::WriteOptions;
use hdfs_native::{client::Client, Result};
use std::collections::HashSet;
use std::io::{BufWriter, Write};
use std::process::{Command, Stdio};
use tempfile::NamedTempFile;
use which::which;
#[cfg(feature = "object_store")]
use {bytes::Bytes, hdfs_native::object_store::HdfsObjectStore};

use hdfs_native::minidfs::{DfsFeatures, MiniDfs};

const TEST_FILE_INTS: usize = 64 * 1024 * 1024;

fn setup(features: &HashSet<DfsFeatures>) -> MiniDfs {
    let hadoop_exc = which("hadoop").expect("Failed to find hadoop executable");

    let dfs = MiniDfs::with_features(features);

    let mut file = NamedTempFile::new_in("target/test").unwrap();
    {
        let mut writer = BufWriter::new(file.as_file_mut());
        for i in 0..TEST_FILE_INTS as i32 {
            let bytes = i.to_be_bytes();
            writer.write(&bytes).unwrap();
        }
        writer.flush().unwrap();
    }

    let status = Command::new(hadoop_exc)
        .args([
            "fs",
            "-copyFromLocal",
            "-f",
            file.path().to_str().unwrap(),
            &format!("{}/testfile", dfs.url),
        ])
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .status()
        .unwrap();
    assert!(status.success());

    dfs
}

pub(crate) async fn test_with_features(features: &HashSet<DfsFeatures>) -> Result<()> {
    let _ = env_logger::builder().is_test(true).try_init();

    let _dfs = setup(features);
    let client = Client::new(&_dfs.url)?;

    test_file_info(&client).await?;
    test_listing(&client).await?;
    test_read(&client).await?;
    test_rename(&client).await?;
    test_dirs(&client).await?;
    test_write(&client).await?;
    // We use writing to create files, so do this after
    test_recursive_listing(&client).await?;

    #[cfg(feature = "object_store")]
    test_object_store(client).await.unwrap();

    Ok(())
}

async fn test_file_info(client: &Client) -> Result<()> {
    let status = client.get_file_info("/testfile").await?;
    // Path is empty, I guess because we already know what file we just got the info for?
    assert_eq!(status.path, "/testfile");
    assert_eq!(status.length, TEST_FILE_INTS * 4);
    Ok(())
}

async fn test_listing(client: &Client) -> Result<()> {
    let statuses = client.list_status("/", false).await?;
    assert_eq!(statuses.len(), 1);
    let status = &statuses[0];
    assert_eq!(status.path, "/testfile");
    assert_eq!(status.length, TEST_FILE_INTS * 4);
    Ok(())
}

async fn test_read(client: &Client) -> Result<()> {
    // Read the whole file
    let reader = client.read("/testfile").await?;
    let buf = reader.read_range(0, TEST_FILE_INTS * 4).await?;
    for i in 0..TEST_FILE_INTS as i32 {
        let mut dst = [0u8; 4];
        let offset = (i * 4) as usize;
        dst.copy_from_slice(&buf.slice(offset..offset + 4)[..]);
        assert_eq!(i32::from_be_bytes(dst), i);
    }

    // Read a single integer from the file
    let buf = reader
        .read_range(TEST_FILE_INTS as usize / 2 * 4, 4)
        .await?;
    let mut dst = [0u8; 4];
    dst.copy_from_slice(&buf[..]);
    assert_eq!(i32::from_be_bytes(dst), TEST_FILE_INTS as i32 / 2);
    Ok(())
}

async fn test_rename(client: &Client) -> Result<()> {
    client.rename("/testfile", "/testfile2", false).await?;

    assert!(client.list_status("/testfile", false).await.is_err());
    assert_eq!(client.list_status("/testfile2", false).await?.len(), 1);

    client.rename("/testfile2", "/testfile", false).await?;
    assert!(client.list_status("/testfile2", false).await.is_err());
    assert_eq!(client.list_status("/testfile", false).await?.len(), 1);

    Ok(())
}

async fn test_dirs(client: &Client) -> Result<()> {
    client.mkdirs("/testdir", 0o755, false).await?;
    assert!(client
        .list_status("/testdir", false)
        .await
        .is_ok_and(|s| s.len() == 0));

    client.delete("/testdir", false).await?;
    assert!(client.list_status("/testdir", false).await.is_err());

    client.mkdirs("/testdir1/testdir2", 0o755, true).await?;
    assert!(client
        .list_status("/testdir1", false)
        .await
        .is_ok_and(|s| s.len() == 1));

    // Deleting non-empty dir without recursive fails
    assert!(client.delete("/testdir1", false).await.is_err());
    assert!(client.delete("/testdir1", true).await.is_ok_and(|r| r));

    Ok(())
}

async fn test_write(client: &Client) -> Result<()> {
    let mut write_options = WriteOptions::default();

    // Create an empty file
    let mut writer = client.create("/newfile", write_options.clone()).await?;

    writer.close().await?;

    assert_eq!(client.get_file_info("/newfile").await?.length, 0);

    // Overwrite now
    write_options.overwrite = true;

    // Check a small files, a file that is exactly one block, and a file slightly bigger than a block
    for size_to_check in [16i32, 128 * 1024 * 1024, 130 * 1024 * 1024] {
        let ints_to_write = size_to_check / 4;

        let mut writer = client.create("/newfile", write_options.clone()).await?;

        let mut data = BytesMut::with_capacity(size_to_check as usize);
        for i in 0..ints_to_write {
            data.put_i32(i);
        }

        let buf = data.freeze();

        writer.write(buf.clone()).await?;
        writer.close().await?;

        assert_eq!(
            client.get_file_info("/newfile").await?.length,
            size_to_check as usize
        );

        let mut reader = client.read("/newfile").await?;
        let read_data = reader.read(reader.file_length()).await?;

        assert_eq!(buf.len(), read_data.len());

        for pos in 0..buf.len() {
            assert_eq!(
                buf[pos], read_data[pos],
                "data is different as position {} for size {}",
                pos, size_to_check
            );
        }
    }

    assert!(client.delete("/newfile", false).await.is_ok_and(|r| r));

    Ok(())
}

async fn test_recursive_listing(client: &Client) -> Result<()> {
    let write_options = WriteOptions::default();
    client.mkdirs("/dir/nested", 0o755, true).await?;
    client
        .create("/dir/file1", write_options.clone())
        .await?
        .close()
        .await?;
    client
        .create("/dir/nested/file2", write_options.clone())
        .await?
        .close()
        .await?;
    client
        .create("/dir/nested/file3", write_options.clone())
        .await?
        .close()
        .await?;

    let statuses = client.list_status("/dir", true).await?;
    assert_eq!(statuses.len(), 4);

    client.delete("/dir", true).await?;

    Ok(())
}

#[cfg(feature = "object_store")]
async fn test_object_store(client: Client) -> object_store::Result<()> {
    let store = HdfsObjectStore::new(client);

    test_object_store_head(&store).await?;
    test_object_store_list(&store).await?;
    test_object_store_rename(&store).await?;
    test_object_store_read(&store).await?;
    test_object_store_write(&store).await?;
    test_object_store_write_multipart(&store).await?;

    Ok(())
}

#[cfg(feature = "object_store")]
async fn test_object_store_head(store: &HdfsObjectStore) -> object_store::Result<()> {
    use object_store::{path::Path, ObjectStore};

    let head = store.head(&Path::from("/testfile")).await?;
    assert_eq!(head.location, Path::from("/testfile"));
    assert_eq!(head.size, TEST_FILE_INTS * 4);

    assert!(store.head(&Path::from("/testfile2")).await.is_err());

    Ok(())
}

#[cfg(feature = "object_store")]
async fn test_object_store_list(store: &HdfsObjectStore) -> object_store::Result<()> {
    use futures::StreamExt;
    use object_store::{path::Path, ObjectMeta, ObjectStore};

    let list: Vec<object_store::Result<ObjectMeta>> =
        store.list(Some(&Path::from("/"))).await?.collect().await;

    assert_eq!(list.len(), 1);
    assert_eq!(list[0].as_ref().unwrap().location, Path::from("/testfile"));

    Ok(())
}

#[cfg(feature = "object_store")]
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

#[cfg(feature = "object_store")]
async fn test_object_store_read(store: &HdfsObjectStore) -> object_store::Result<()> {
    use object_store::{path::Path, ObjectStore};

    let location = Path::from("/testfile");

    let buf = store.get(&location).await?.bytes().await?;
    for i in 0..TEST_FILE_INTS as i32 {
        let mut dst = [0u8; 4];
        let offset = (i * 4) as usize;
        dst.copy_from_slice(&buf.slice(offset..offset + 4)[..]);
        assert_eq!(i32::from_be_bytes(dst), i);
    }

    // Read a single integer from the file
    let offset = TEST_FILE_INTS as usize / 2 * 4;
    let buf = store.get_range(&location, offset..(offset + 4)).await?;
    let mut dst = [0u8; 4];
    dst.copy_from_slice(&buf[..]);
    assert_eq!(i32::from_be_bytes(dst), TEST_FILE_INTS as i32 / 2);
    Ok(())
}

#[cfg(feature = "object_store")]
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

#[cfg(feature = "object_store")]
async fn test_object_store_write_multipart(store: &HdfsObjectStore) -> object_store::Result<()> {
    use bytes::Buf;
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
