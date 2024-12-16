Usage
=====

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