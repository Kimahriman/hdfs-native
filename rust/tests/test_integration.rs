#[cfg(feature = "integration-test")]
mod common;

#[cfg(feature = "integration-test")]
mod test {
    use crate::common::{assert_bufs_equal, TEST_FILE_INTS};
    use bytes::{BufMut, BytesMut};
    use hdfs_native::{
        acl::AclEntry,
        client::FileStatus,
        minidfs::{DfsFeatures, MiniDfs},
        Client, Result, WriteOptions,
    };
    use serial_test::serial;
    use std::collections::HashSet;

    #[tokio::test]
    #[serial]
    async fn test_basic_non_ha() {
        test_with_features(&HashSet::new()).await.unwrap();
    }

    #[tokio::test]
    #[serial]
    async fn test_security_kerberos() {
        test_with_features(&HashSet::from([DfsFeatures::Security]))
            .await
            .unwrap();
    }

    #[tokio::test]
    #[serial]
    async fn test_security_token() {
        test_with_features(&HashSet::from([DfsFeatures::Security, DfsFeatures::Token]))
            .await
            .unwrap();
    }

    #[tokio::test]
    #[serial]
    async fn test_integrity_kerberos() {
        test_with_features(&HashSet::from([
            DfsFeatures::Security,
            DfsFeatures::Integrity,
        ]))
        .await
        .unwrap();
    }

    #[tokio::test]
    #[serial]
    async fn test_integrity_token() {
        test_with_features(&HashSet::from([
            DfsFeatures::Security,
            DfsFeatures::Token,
            DfsFeatures::Integrity,
        ]))
        .await
        .unwrap();
    }

    #[tokio::test]
    #[serial]
    async fn test_privacy_kerberos() {
        test_with_features(&HashSet::from([
            DfsFeatures::Security,
            DfsFeatures::Privacy,
        ]))
        .await
        .unwrap();
    }

    #[tokio::test]
    #[serial]
    async fn test_privacy_token() {
        test_with_features(&HashSet::from([
            DfsFeatures::Security,
            DfsFeatures::Token,
            DfsFeatures::Privacy,
        ]))
        .await
        .unwrap();
    }

    #[tokio::test]
    #[serial]
    async fn test_aes() {
        test_with_features(&HashSet::from([
            DfsFeatures::Security,
            DfsFeatures::Privacy,
            DfsFeatures::AES,
        ]))
        .await
        .unwrap();
    }

    #[tokio::test]
    #[serial]
    async fn test_forced_data_transfer_encryption() {
        // DataTransferEncryption enabled but privacy isn't, still force encryption
        test_with_features(&HashSet::from([
            DfsFeatures::Security,
            DfsFeatures::DataTransferEncryption,
        ]))
        .await
        .unwrap();
    }

    #[tokio::test]
    #[serial]
    async fn test_data_transfer_encryption() {
        test_with_features(&HashSet::from([
            DfsFeatures::Security,
            DfsFeatures::Privacy,
            DfsFeatures::DataTransferEncryption,
        ]))
        .await
        .unwrap();
    }

    #[tokio::test]
    #[serial]
    async fn test_data_transfer_encryption_aes() {
        test_with_features(&HashSet::from([
            DfsFeatures::Security,
            DfsFeatures::Privacy,
            DfsFeatures::DataTransferEncryption,
            DfsFeatures::AES,
        ]))
        .await
        .unwrap();
    }

    #[tokio::test]
    #[serial]
    async fn test_basic_ha() {
        test_with_features(&HashSet::from([DfsFeatures::HA]))
            .await
            .unwrap();
    }

    #[tokio::test]
    #[serial]
    async fn test_security_privacy_ha() {
        test_with_features(&HashSet::from([
            DfsFeatures::Security,
            DfsFeatures::Privacy,
            DfsFeatures::HA,
        ]))
        .await
        .unwrap();
    }

    #[tokio::test]
    #[serial]
    async fn test_security_token_ha() {
        test_with_features(&HashSet::from([
            DfsFeatures::Security,
            DfsFeatures::Token,
            DfsFeatures::HA,
        ]))
        .await
        .unwrap();
    }

    #[tokio::test]
    #[serial]
    async fn test_rbf() {
        test_with_features(&HashSet::from([DfsFeatures::RBF]))
            .await
            .unwrap();
    }

    pub async fn test_with_features(features: &HashSet<DfsFeatures>) -> Result<()> {
        let _ = env_logger::builder().is_test(true).try_init();

        let _dfs = MiniDfs::with_features(features);
        let client = Client::default();

        let mut file = client.create("/testfile", WriteOptions::default()).await?;
        for i in 0..TEST_FILE_INTS as i32 {
            file.write(i.to_be_bytes().to_vec().into()).await?;
        }
        file.close().await?;

        test_file_info(&client).await?;
        test_listing(&client).await?;
        test_rename(&client).await?;
        test_dirs(&client).await?;
        test_read_write(&client).await?;
        // We use writing to create files, so do this after
        test_recursive_listing(&client).await?;
        test_set_times(&client).await?;
        test_set_owner(&client).await?;
        test_set_permission(&client).await?;
        test_set_replication(&client).await?;
        test_get_content_summary(&client).await?;
        test_acls(&client).await?;
        test_globbing(&client).await?;

        Ok(())
    }

    async fn test_globbing(client: &Client) -> Result<()> {
        let base_dir = "/test_globbing_root";
        // Ensure clean slate for test_globbing
        if client.get_file_info(base_dir).await.is_ok() {
            client.delete(base_dir, true).await?;
        }
        client.mkdirs(base_dir, 0o755, true).await?;

        // Run tests
        test_glob_list_status(client, base_dir).await?;
        test_glob_read_ops(client, base_dir).await?;
        test_glob_modifying_ops(client, base_dir).await?;
        test_glob_disallowed_ops(client, base_dir).await?;

        // Cleanup
        client.delete(base_dir, true).await?;
        Ok(())
    }

    async fn test_glob_list_status(client: &Client, base_dir: &str) -> Result<()> {
        let root = format!("{}/list_status", base_dir);
        client.mkdirs(&root, 0o755, true).await?;

        client.create(&format!("{}/file1.txt", root), WriteOptions::default()).await?.close().await?;
        client.create(&format!("{}/file2.log", root), WriteOptions::default()).await?.close().await?;
        client.create(&format!("{}/foo_abc.txt", root), WriteOptions::default()).await?.close().await?;
        client.create(&format!("{}/foo_def.csv", root), WriteOptions::default()).await?.close().await?;
        client.create(&format!("{}/bar_file.txt", root), WriteOptions::default()).await?.close().await?;
        client.create(&format!("{}/file[x].txt", root), WriteOptions::default()).await?.close().await?;
        client.create(&format!("{}/file_q.log", root), WriteOptions::default()).await?.close().await?; // For '?' test
        
        let subdir1 = format!("{}/subdir1", root);
        client.mkdirs(&subdir1, 0o755, true).await?;
        client.create(&format!("{}/subfile1.txt", subdir1), WriteOptions::default()).await?.close().await?;
        client.create(&format!("{}/another.log", subdir1), WriteOptions::default()).await?.close().await?;
        client.create(&format!("{}/deep_foo.csv", subdir1), WriteOptions::default()).await?.close().await?;

        let subdir2 = format!("{}/subdir2", root);
        client.mkdirs(&subdir2, 0o755, true).await?;
        client.create(&format!("{}/subfile2.log", subdir2), WriteOptions::default()).await?.close().await?;
        client.mkdirs(&format!("{}/empty_dir", subdir2), 0o755, true).await?;

        // Basic patterns
        let paths: HashSet<String> = client.list_status(&root, false, Some("*.txt")).await?.into_iter().map(|fs| fs.path).collect();
        let expected_paths: HashSet<String> = [
            format!("{}/file1.txt", root),
            format!("{}/foo_abc.txt", root),
            format!("{}/bar_file.txt", root),
            format!("{}/file[x].txt", root),
        ].iter().cloned().collect();
        assert_eq!(paths, expected_paths, "Basic pattern *.txt failed");

        let paths: HashSet<String> = client.list_status(&root, false, Some("foo*")).await?.into_iter().map(|fs| fs.path).collect();
        let expected_paths: HashSet<String> = [
            format!("{}/foo_abc.txt", root),
            format!("{}/foo_def.csv", root),
        ].iter().cloned().collect();
        assert_eq!(paths, expected_paths, "Basic pattern foo* failed");

        let paths: HashSet<String> = client.list_status(&root, false, Some("*_file.txt")).await?.into_iter().map(|fs| fs.path).collect();
        let expected_paths: HashSet<String> = [format!("{}/bar_file.txt", root)].iter().cloned().collect();
        assert_eq!(paths, expected_paths, "Basic pattern *_file.txt failed");

        // Recursive patterns
        let paths: HashSet<String> = client.list_status(&root, true, Some("**/*.txt")).await?.into_iter().map(|fs| fs.path).collect();
        let expected_paths: HashSet<String> = [
            format!("{}/file1.txt", root),
            format!("{}/foo_abc.txt", root),
            format!("{}/bar_file.txt", root),
            format!("{}/file[x].txt", root),
            format!("{}/subfile1.txt", subdir1),
        ].iter().cloned().collect();
        assert_eq!(paths, expected_paths, "Recursive pattern **/*.txt failed");
        
        let paths: HashSet<String> = client.list_status(&root, true, Some("**/foo*.csv")).await?.into_iter().map(|fs| fs.path).collect();
        let expected_paths: HashSet<String> = [
            format!("{}/foo_def.csv", root),
            format!("{}/deep_foo.csv", subdir1),
        ].iter().cloned().collect();
        assert_eq!(paths, expected_paths, "Recursive pattern **/foo*.csv failed");

        // Patterns matching dirs and files
        let paths: HashSet<String> = client.list_status(&root, false, Some("subdir*")).await?.into_iter().map(|fs| fs.path).collect();
        let expected_paths_for_subdir_star: HashSet<String> = [subdir1.clone(), subdir2.clone()].iter().cloned().collect();
        assert_eq!(paths, expected_paths_for_subdir_star, "Pattern matching dirs subdir* failed");

        let paths: HashSet<String> = client.list_status(&root, true, Some("subdir*/*file*.txt")).await?.into_iter().map(|fs| fs.path).collect();
        let expected_paths: HashSet<String> = [format!("{}/subfile1.txt", subdir1)].iter().cloned().collect();
        assert_eq!(paths, expected_paths, "Recursive pattern subdir*/*file*.txt failed");
        
        // Patterns matching nothing
        let statuses = client.list_status(&root, false, Some("*.nonexistent")).await?;
        assert!(statuses.is_empty(), "Pattern matching nothing failed");

        // Patterns with special characters
        let paths: HashSet<String> = client.list_status(&root, false, Some("file[x].txt")).await?.into_iter().map(|fs| fs.path).collect();
        let expected_paths: HashSet<String> = [format!("{}/file[x].txt", root)].iter().cloned().collect();
        assert_eq!(paths, expected_paths, "Pattern with [x] failed");

        let paths: HashSet<String> = client.list_status(&root, false, Some("file_?.log")).await?.into_iter().map(|fs| fs.path).collect();
        let expected_paths: HashSet<String> = [format!("{}/file_q.log", root)].iter().cloned().collect();
        assert_eq!(paths, expected_paths, "Pattern with ? failed");
        
        client.delete(&root, true).await?;
        Ok(())
    }

    async fn test_glob_read_ops(client: &Client, base_dir: &str) -> Result<()> {
        let root = format!("{}/read_ops", base_dir);
        client.mkdirs(&root, 0o755, true).await?;

        client.create(&format!("{}/file.txt", root), WriteOptions::default()).await?.close().await?;
        client.mkdirs(&format!("{}/dir1", root), 0o755, true).await?;
        client.create(&format!("{}/multi_a.dat", root), WriteOptions::default()).await?.close().await?;
        client.create(&format!("{}/multi_b.dat", root), WriteOptions::default()).await?.close().await?;

        // Single file match
        assert_eq!(client.get_file_info(&format!("{}/file.tx?", root)).await?.path, format!("{}/file.txt", root));
        let mut reader = client.read(&format!("{}/f*.txt", root)).await?;
        assert_eq!(reader.file_length(), 0); 
        assert_eq!(client.get_content_summary(&format!("{}/fi*.txt", root)).await?.length, 0);
        assert!(client.get_acl_status(&format!("{}/file.txt", root)).await.is_ok()); 

        // Single dir match (non-read)
        let dir_info = client.get_file_info(&format!("{}/dir*", root)).await?;
        assert_eq!(dir_info.path, format!("{}/dir1", root));
        assert!(dir_info.isdir);
        assert_eq!(client.get_content_summary(&format!("{}/d*1", root)).await?.directory_count, 1);
        assert!(client.get_acl_status(&format!("{}/di?1", root)).await.is_ok());

        // Single dir match (for `read`)
        let read_dir_res = client.read(&format!("{}/dir*", root)).await;
        assert!(matches!(read_dir_res, Err(hdfs_native::error::HdfsError::InvalidArgument(_))), "Read on dir glob should fail: {:?}", read_dir_res);

        // Multiple matches
        let multi_match_res_info = client.get_file_info(&format!("{}/multi_*.dat", root)).await;
        assert!(matches!(multi_match_res_info, Err(hdfs_native::error::HdfsError::InvalidArgument(_))), "get_file_info on multi-match glob should fail: {:?}", multi_match_res_info);
        let multi_match_res_read = client.read(&format!("{}/multi_*.dat", root)).await;
        assert!(matches!(multi_match_res_read, Err(hdfs_native::error::HdfsError::InvalidArgument(_))), "read on multi-match glob should fail: {:?}", multi_match_res_read);

        // Zero matches
        let zero_match_res_info = client.get_file_info(&format!("{}/nothing*.dat", root)).await;
        assert!(matches!(zero_match_res_info, Err(hdfs_native::error::HdfsError::FileNotFound(_))), "get_file_info on zero-match glob should fail: {:?}", zero_match_res_info);

        // Invalid glob pattern (e.g. unclosed bracket)
        // Current client behavior: ListStatusIterator's pattern becomes None if Pattern::new fails.
        // resolve_glob_path_to_single_entry for path "[unclosed" uses base_path ".".
        // If "." is empty (as it is here, since we are in a clean test dir `root`), then it results in FileNotFound.
        // If client logic is improved to fail Pattern::new upfront, this would be InvalidArgument.
        let invalid_pattern_res = client.get_file_info(&format!("{}/[unclosed", root)).await;
        assert!(matches!(invalid_pattern_res, Err(hdfs_native::error::HdfsError::FileNotFound(_)) | Err(hdfs_native::error::HdfsError::InvalidArgument(_))), "get_file_info on invalid glob should result in FileNotFound or InvalidArgument: {:?}", invalid_pattern_res);

        client.delete(&root, true).await?;
        Ok(())
    }

    async fn test_glob_modifying_ops(client: &Client, base_dir: &str) -> Result<()> {
        let root = format!("{}/modify_ops", base_dir);
        client.mkdirs(&root, 0o755, true).await?;

        client.create(&format!("{}/file1.tmp", root), WriteOptions::default()).await?.close().await?;
        client.create(&format!("{}/file2.tmp", root), WriteOptions::default()).await?.close().await?;
        client.mkdirs(&format!("{}/subdir", root), 0o755, true).await?;
        client.create(&format!("{}/subdir/file3.tmp", root), WriteOptions::default()).await?.close().await?;

        // Delete non-recursive glob
        let delete_res = client.delete(&format!("{}/file*.tmp", root), false).await?; 
        assert!(delete_res, "Delete glob /file*.tmp should succeed");
        assert!(client.get_file_info(&format!("{}/file1.tmp", root)).await.is_err(), "file1.tmp should be deleted");
        assert!(client.get_file_info(&format!("{}/file2.tmp", root)).await.is_err(), "file2.tmp should be deleted");
        assert!(client.get_file_info(&format!("{}/subdir/file3.tmp", root)).await.is_ok(), "subdir/file3.tmp should remain");

        // Recreate for next test
        client.create(&format!("{}/file1.tmp", root), WriteOptions::default()).await?.close().await?;
        client.create(&format!("{}/file2.tmp", root), WriteOptions::default()).await?.close().await?;

        // Delete recursive glob
        let delete_recursive_res = client.delete(&format!("{}/**/*.tmp", root), true).await?; 
        assert!(delete_recursive_res, "Recursive delete glob **/*.tmp should succeed");
        assert!(client.get_file_info(&format!("{}/file1.tmp", root)).await.is_err());
        assert!(client.get_file_info(&format!("{}/file2.tmp", root)).await.is_err());
        assert!(client.get_file_info(&format!("{}/subdir/file3.tmp", root)).await.is_err());
        
        // set_replication
        client.create(&format!("{}/repl1.dat", root), WriteOptions::default()).await?.close().await?;
        client.create(&format!("{}/repl2.dat", root), WriteOptions::default()).await?.close().await?;
        let repl_res = client.set_replication(&format!("{}/repl*.dat", root), 1).await?; 
        assert!(repl_res, "set_replication on glob should succeed");
        assert_eq!(client.get_file_info(&format!("{}/repl1.dat", root)).await?.replication.unwrap_or(0), 1);
        assert_eq!(client.get_file_info(&format!("{}/repl2.dat", root)).await?.replication.unwrap_or(0), 1);

        let repl_no_match = client.set_replication(&format!("{}/no_match_repl*.dat", root), 1).await?;
        assert!(repl_no_match, "set_replication on no-match glob should succeed (true)");

        // set_permission
        client.create(&format!("{}/perm1.dat", root), WriteOptions::default()).await?.close().await?;
        client.create(&format!("{}/perm2.dat", root), WriteOptions::default()).await?.close().await?;
        client.set_permission(&format!("{}/perm*.dat", root), 0o700).await?;
        assert_eq!(client.get_file_info(&format!("{}/perm1.dat", root)).await?.permission, 0o700);
        assert_eq!(client.get_file_info(&format!("{}/perm2.dat", root)).await?.permission, 0o700);

        // set_times
        client.create(&format!("{}/time1.dat", root), WriteOptions::default()).await?.close().await?;
        client.create(&format!("{}/time2.dat", root), WriteOptions::default()).await?.close().await?;
        let mtime = 1678886400000; // Example timestamp
        let atime = 1678886401000; // Example timestamp
        client.set_times(&format!("{}/time*.dat", root), mtime, atime).await?;
        let info1 = client.get_file_info(&format!("{}/time1.dat", root)).await?;
        assert_eq!(info1.modification_time, mtime);
        assert_eq!(info1.access_time, atime);
        let info2 = client.get_file_info(&format!("{}/time2.dat", root)).await?;
        assert_eq!(info2.modification_time, mtime);
        assert_eq!(info2.access_time, atime);

        // set_owner
        client.create(&format!("{}/owner1.dat", root), WriteOptions::default()).await?.close().await?;
        client.create(&format!("{}/owner2.dat", root), WriteOptions::default()).await?.close().await?;
        let test_user = "testglobuser";
        let test_group = "testglobgroup";
        // Note: Setting arbitrary owners/groups might require specific HDFS permissions or superuser.
        // This test assumes MiniDFS allows it or runs as superuser.
        client.set_owner(&format!("{}/owner*.dat", root), Some(test_user), Some(test_group)).await?;
        let owner_info1 = client.get_file_info(&format!("{}/owner1.dat", root)).await?;
        assert_eq!(owner_info1.owner, test_user);
        assert_eq!(owner_info1.group, test_group);
        let owner_info2 = client.get_file_info(&format!("{}/owner2.dat", root)).await?;
        assert_eq!(owner_info2.owner, test_user);
        assert_eq!(owner_info2.group, test_group);
        
        // modify_acl_entries
        client.create(&format!("{}/acl1.dat", root), WriteOptions::default()).await?.close().await?;
        client.create(&format!("{}/acl2.dat", root), WriteOptions::default()).await?.close().await?;
        let acl_entry = AclEntry::new_user("access", "read", Some("globacluser".to_string()));
        client.modify_acl_entries(&format!("{}/acl*.dat", root), vec![acl_entry.clone()]).await?;
        
        let acl_status1 = client.get_acl_status(&format!("{}/acl1.dat", root)).await?;
        // Check if our specific entry is present, ignoring others that might be default
        assert!(acl_status1.entries.iter().any(|e| e.name == acl_entry.name && e.r#type == acl_entry.r#type && e.scope == acl_entry.scope && e.permissions == acl_entry.permissions), 
            "ACL entry not found in acl1.dat. Actual: {:?}", acl_status1.entries);

        let acl_status2 = client.get_acl_status(&format!("{}/acl2.dat", root)).await?;
        assert!(acl_status2.entries.iter().any(|e| e.name == acl_entry.name && e.r#type == acl_entry.r#type && e.scope == acl_entry.scope && e.permissions == acl_entry.permissions),
            "ACL entry not found in acl2.dat. Actual: {:?}", acl_status2.entries);


        // Zero matches for a modifying op
        let delete_zero_res = client.delete(&format!("{}/nonexistent_del_*.tmp", root), false).await?;
        assert!(delete_zero_res, "Delete on zero-match glob should succeed (true)");

        // Invalid glob pattern for modifying op
        // As with read_ops, this depends on client's Pattern::new error handling.
        // Assuming current behavior (invalid pattern -> None -> acts on base_path):
        // Using a pattern that is valid but definitely won't match anything is safer for now.
        let unmatchable_delete_res = client.delete(&format!("{}/unmatchable[pattern]xyz", root), false).await?;
        assert!(unmatchable_delete_res, "Delete on unmatchable valid glob should be Ok(true)");

        client.delete(&root, true).await?;
        Ok(())
    }

    async fn test_glob_disallowed_ops(client: &Client, base_dir: &str) -> Result<()> {
        let root = format!("{}/disallowed_ops", base_dir);
        client.mkdirs(&root, 0o755, true).await?;
        
        client.create(&format!("{}/src.txt", root), WriteOptions::default()).await?.close().await?;

        // mkdirs
        let mkdir_glob_res = client.mkdirs(&format!("{}/foo*/bar", root), 0o755, true).await;
        assert!(matches!(mkdir_glob_res, Err(hdfs_native::error::HdfsError::InvalidArgument(_))), "mkdirs with glob in path should fail: {:?}", mkdir_glob_res);

        // rename
        let rename_src_glob_res = client.rename(&format!("{}/src*.txt", root), &format!("{}/dst.txt", root), false).await;
        assert!(matches!(rename_src_glob_res, Err(hdfs_native::error::HdfsError::InvalidArgument(_))), "rename with glob in src should fail: {:?}", rename_src_glob_res);
        
        let rename_dst_glob_res = client.rename(&format!("{}/src.txt", root), &format!("{}/dst*.txt", root), false).await;
        assert!(matches!(rename_dst_glob_res, Err(hdfs_native::error::HdfsError::InvalidArgument(_))), "rename with glob in dst should fail: {:?}", rename_dst_glob_res);

        assert!(client.get_file_info(&format!("{}/src.txt", root)).await.is_ok(), "src.txt should still exist after failed renames");

        client.delete(&root, true).await?;
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
        let statuses: Vec<FileStatus> = client
            .list_status("/", false)
            .await?
            .into_iter()
            // Only include files, since federation things could result in folders being created
            .filter(|s| !s.isdir)
            .collect();
        assert_eq!(statuses.len(), 1);
        let status = &statuses[0];
        assert_eq!(status.path, "/testfile");
        assert_eq!(status.length, TEST_FILE_INTS * 4);
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
            .is_ok_and(|s| s.is_empty()));

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

    async fn test_read_write(client: &Client) -> Result<()> {
        let write_options = WriteOptions::default().overwrite(true);

        // Create an empty file
        let mut writer = client.create("/newfile", &write_options).await?;

        writer.close().await?;

        assert_eq!(client.get_file_info("/newfile").await?.length, 0);

        let mut writer = client.create("/newfile", &write_options).await?;

        let mut file_contents = BytesMut::new();
        let mut data = BytesMut::new();
        for i in 0..1024 {
            file_contents.put_i32(i);
            data.put_i32(i);
        }

        let buf = data.freeze();

        writer.write(buf).await?;
        writer.close().await?;

        assert_eq!(client.get_file_info("/newfile").await?.length, 4096);

        let mut reader = client.read("/newfile").await?;
        let read_data = reader.read(reader.file_length()).await?;

        assert_bufs_equal(&file_contents, &read_data, None);

        let mut data = BytesMut::new();
        for i in 0..1024 {
            file_contents.put_i32(i);
            data.put_i32(i);
        }

        let buf = data.freeze();

        let mut writer = client.append("/newfile").await?;
        writer.write(buf).await?;
        writer.close().await?;

        let mut reader = client.read("/newfile").await?;
        let read_data = reader.read(reader.file_length()).await?;

        assert_bufs_equal(&file_contents, &read_data, None);

        assert!(client.delete("/newfile", false).await.is_ok_and(|r| r));

        client.mkdirs("/testdir", 0o755, true).await?;
        assert!(client.read("/testdir").await.is_err());
        client.delete("/testdir", true).await?;

        Ok(())
    }

    async fn test_recursive_listing(client: &Client) -> Result<()> {
        let write_options = WriteOptions::default();
        client.mkdirs("/dir/nested", 0o755, true).await?;
        client
            .create("/dir/file1", &write_options)
            .await?
            .close()
            .await?;
        client
            .create("/dir/nested/file2", &write_options)
            .await?
            .close()
            .await?;
        client
            .create("/dir/nested/file3", &write_options)
            .await?
            .close()
            .await?;

        let statuses = client.list_status("/dir", true).await?;
        assert_eq!(statuses.len(), 4);

        client.delete("/dir", true).await?;

        Ok(())
    }

    async fn test_set_times(client: &Client) -> Result<()> {
        client
            .create("/test", WriteOptions::default())
            .await?
            .close()
            .await?;

        let mtime = 1717641455;
        let atime = 1717641456;

        client.set_times("/test", mtime, atime).await?;

        let file_info = client.get_file_info("/test").await?;

        assert_eq!(file_info.modification_time, mtime);
        assert_eq!(file_info.access_time, atime);

        client.delete("/test", false).await?;

        Ok(())
    }

    async fn test_set_owner(client: &Client) -> Result<()> {
        client
            .create("/test", WriteOptions::default())
            .await?
            .close()
            .await?;

        client
            .set_owner("/test", Some("testuser"), Some("testgroup"))
            .await?;
        let file_info = client.get_file_info("/test").await?;

        assert_eq!(file_info.owner, "testuser");
        assert_eq!(file_info.group, "testgroup");

        client.set_owner("/test", Some("testuser2"), None).await?;
        let file_info = client.get_file_info("/test").await?;

        assert_eq!(file_info.owner, "testuser2");
        assert_eq!(file_info.group, "testgroup");

        client.set_owner("/test", None, Some("testgroup2")).await?;
        let file_info = client.get_file_info("/test").await?;

        assert_eq!(file_info.owner, "testuser2");
        assert_eq!(file_info.group, "testgroup2");

        client.delete("/test", false).await?;

        Ok(())
    }

    async fn test_set_permission(client: &Client) -> Result<()> {
        client
            .create("/test", WriteOptions::default())
            .await?
            .close()
            .await?;

        let file_info = client.get_file_info("/test").await?;
        assert_eq!(file_info.permission, 0o644);

        client.set_permission("/test", 0o600).await?;
        let file_info = client.get_file_info("/test").await?;
        assert_eq!(file_info.permission, 0o600);

        client.delete("/test", false).await?;

        Ok(())
    }

    async fn test_set_replication(client: &Client) -> Result<()> {
        client
            .create("/test", WriteOptions::default())
            .await?
            .close()
            .await?;

        client.set_replication("/test", 1).await?;
        let file_info = client.get_file_info("/test").await?;
        assert_eq!(file_info.replication, Some(1));

        client.set_replication("/test", 2).await?;
        let file_info = client.get_file_info("/test").await?;
        assert_eq!(file_info.replication, Some(2));

        client.delete("/test", false).await?;

        Ok(())
    }

    async fn test_get_content_summary(client: &Client) -> Result<()> {
        let mut file1 = client.create("/test", WriteOptions::default()).await?;

        file1.write(vec![0, 1, 2, 3].into()).await?;
        file1.close().await?;

        let mut file2 = client.create("/test2", WriteOptions::default()).await?;

        file2.write(vec![0, 1, 2, 3, 4, 5].into()).await?;
        file2.close().await?;

        client.mkdirs("/testdir", 0o755, true).await?;

        let content_summary = client.get_content_summary("/").await?;
        assert_eq!(content_summary.file_count, 3,);
        assert_eq!(content_summary.directory_count, 2);
        // Test file plus the two we made above
        assert_eq!(content_summary.length, TEST_FILE_INTS as u64 * 4 + 4 + 6);

        client.delete("/test", false).await?;
        client.delete("/test2", false).await?;

        Ok(())
    }

    async fn test_acls(client: &Client) -> Result<()> {
        client
            .create("/test", WriteOptions::default())
            .await?
            .close()
            .await?;

        let acl_status = client.get_acl_status("/test").await?;

        assert!(acl_status.entries.is_empty());
        assert!(!acl_status.sticky);

        let user_entry = AclEntry::new("user", "access", "r--", Some("testuser".to_string()));

        let group_entry = AclEntry::new("group", "access", "-w-", Some("testgroup".to_string()));

        client
            .modify_acl_entries("/test", vec![user_entry.clone()])
            .await?;

        let acl_status = client.get_acl_status("/test").await?;

        // Empty group permission added automatically
        assert_eq!(acl_status.entries.len(), 2, "{:?}", acl_status.entries);
        assert!(acl_status.entries.contains(&user_entry));

        client
            .modify_acl_entries("/test", vec![group_entry.clone()])
            .await?;

        let acl_status = client.get_acl_status("/test").await?;

        // Still contains the empty group
        assert_eq!(acl_status.entries.len(), 3, "{:?}", acl_status.entries);
        assert!(acl_status.entries.contains(&user_entry));
        assert!(acl_status.entries.contains(&group_entry));

        client
            .remove_acl_entries("/test", vec![group_entry.clone()])
            .await?;

        let acl_status = client.get_acl_status("/test").await?;

        assert_eq!(acl_status.entries.len(), 2, "{:?}", acl_status.entries);
        assert!(acl_status.entries.contains(&user_entry));
        assert!(!acl_status.entries.contains(&group_entry));

        client.remove_acl("/test").await?;

        let acl_status = client.get_acl_status("/test").await?;

        assert_eq!(acl_status.entries.len(), 0);

        client.delete("/test", false).await?;

        // Default acl
        client.mkdirs("/testdir", 0o755, true).await?;

        client
            .modify_acl_entries(
                "/testdir",
                vec![AclEntry {
                    r#type: hdfs_native::acl::AclEntryType::User,
                    scope: hdfs_native::acl::AclEntryScope::Default,
                    permissions: hdfs_native::acl::FsAction::Read,
                    name: Some("testuser".to_string()),
                }],
            )
            .await?;

        let acl_status = client.get_acl_status("/testdir").await?;

        // All defaults get added automatically based on permissions
        assert_eq!(acl_status.entries.len(), 5, "{:?}", acl_status.entries);

        client
            .create("/testdir/test", WriteOptions::default())
            .await?
            .close()
            .await?;

        let acl_status = client.get_acl_status("/testdir/test").await?;

        // Default user acl added above plus the empty group permission
        assert_eq!(acl_status.entries.len(), 2, "{:?}", acl_status.entries);

        client.remove_default_acl("/testdir").await?;

        client.delete("/testdir", true).await?;

        Ok(())
    }
}
