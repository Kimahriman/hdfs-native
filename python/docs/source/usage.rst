Usage
=====

Client API
----------

Simply create a :py:class:`Client <hdfs_native.Client>`.

.. code-block::

    from hdfs_native import Client

    # Load the fs.defaultFS from your core-site.xml config file
    client = Client()

    # Connect to a specific NameNode
    client = Client("hdfs://nn-1:9000")

    # Connect to a Name Service
    client = Client("hdfs://ns")

See :py:class:`Client <hdfs_native.Client>` for supported methods on a client.

Common Operations
~~~~~~~~~~~~~~~~~

Reading Files
^^^^^^^^^^^^^

.. code-block::

    from hdfs_native import Client

    client = Client("hdfs://localhost:9000")

    # Check if file exists
    status = client.get_file_info("/path/to/file.txt")
    print(f"File size: {status.length} bytes")

    # Read entire file
    with client.read("/path/to/file.txt") as f:
        content = f.read()

    # Read file with offset
    with client.read("/path/to/file.txt") as f:
        f.seek(100)
        chunk = f.read(1024)

Writing Files
^^^^^^^^^^^^^

.. code-block::

    from hdfs_native import Client, WriteOptions

    client = Client("hdfs://localhost:9000")

    # Write new file
    with client.create("/path/to/newfile.txt") as f:
        f.write(b"Hello, HDFS!")

    # Append to existing file
    with client.append("/path/to/file.txt") as f:
        f.write(b"\nAppended content")

    # Write with custom options
    write_opts = WriteOptions(overwrite=True, replication=3)
    with client.create("/path/to/file.txt", write_options=write_opts) as f:
        f.write(b"Data with replication factor of 3")

Directory Operations
^^^^^^^^^^^^^^^^^^^^

.. code-block::

    from hdfs_native import Client

    client = Client("hdfs://localhost:9000")

    # List directory
    entries = client.list_status("/path/to/dir")
    for entry in entries:
        print(f"{entry.path} - Size: {entry.length}")

    # Create directory
    client.mkdir("/path/to/newdir")

    # Create directory recursively
    client.mkdirs("/path/to/nested/dir")

    # Delete directory
    client.delete("/path/to/dir", recursive=True)

    # Get directory summary
    summary = client.get_content_summary("/path/to/dir")
    print(f"Total size: {summary.length} bytes")
    print(f"File count: {summary.file_count}")

File Metadata
^^^^^^^^^^^^^

.. code-block::

    from hdfs_native import Client

    client = Client("hdfs://localhost:9000")

    # Get file status
    status = client.get_file_info("/path/to/file.txt")
    print(f"Owner: {status.owner}")
    print(f"Group: {status.group}")
    print(f"Permissions: {status.permission}")
    print(f"Modification time: {status.modification_time}")

    # Set permissions
    client.set_permission("/path/to/file.txt", 0o644)

    # Get ACL status
    acl = client.get_acl_status("/path/to/file.txt")
    for entry in acl.entries:
        print(entry)

Async Operations
~~~~~~~~~~~~~~~~

For async applications, use :py:class:`AsyncClient <hdfs_native.AsyncClient>`.

.. code-block::

    import asyncio
    from hdfs_native import AsyncClient

    async def main():
        client = AsyncClient("hdfs://localhost:9000")

        # Read file asynchronously
        f = await client.read("/path/to/file.txt")
        async with f:
            data = await f.read()

        # List directory asynchronously
        async for entry in client.list_status("/path/to/dir"):
            print(entry.path)

        # Create file asynchronously
        f = await client.create("/path/to/newfile.txt")
        async with f:
            await f.write(b"Async write")

    asyncio.run(main())

CLI Usage
---------

The package includes a built-in CLI tool ``hdfsn`` that implements most of the behavior of ``hdfs dfs`` with a more bash-like syntax.

Installation
~~~~~~~~~~~~

The easiest way to install the CLI is with UV:

.. code-block::

    uv tool install hdfs-native

Alternatively, you can install via pip:

.. code-block::

    pip install hdfs-native
    hdfsn --help

Basic Commands
~~~~~~~~~~~~~~

.. code-block::

    # List files
    hdfsn ls /path/to/dir

    # Create directory
    hdfsn mkdir /path/to/newdir

    # Upload file
    hdfsn put local_file.txt /hdfs/path/

    # Download file
    hdfsn get /hdfs/path/file.txt local_file.txt

    # Remove file or directory
    hdfsn rm /path/to/file.txt
    hdfsn rm -r /path/to/dir

    # Display file contents
    hdfsn cat /path/to/file.txt

    # Copy within HDFS
    hdfsn cp /source/path /dest/path

    # Move within HDFS
    hdfsn mv /source/path /dest/path

Auto-complete Support
~~~~~~~~~~~~~~~~~~~~~

The CLI supports shell auto-completion for HDFS paths using ``argcomplete``. There are two ways to enable this support:

**Option 1: Global auto-complete for all Python tools**

.. code-block::

    uv tool install argcomplete
    activate-global-python-argcomplete

This enables auto-complete for all Python command-line tools that support ``argcomplete``.

**Option 2: Shell-specific auto-complete for hdfsn only**

.. code-block::

    uv tool install argcomplete
    eval "$(register-python-argcomplete hdfsn)"

Add this command to your shell's configuration file (``.bashrc``, ``.zshrc``, etc.) to make it persistent.

Once enabled, you can use Tab to auto-complete HDFS paths:

.. code-block::

    $ hdfsn ls /data/<TAB>
    /data/users
    /data/logs

    $ hdfsn cat /data/file<TAB>
    /data/file.txt
    /data/file.csv