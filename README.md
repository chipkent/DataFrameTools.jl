# DataFrameTools.jl
Julia package containing utility functions for working with DataFrames.
For example, DataFrameTools supports serialization and deserialization of
DataFrames into many formats.  This allows the DataFrame serialization format to be
easily changed to achieve smaller file sizes, faster file reads, or compatibility with
other applications.

Key functions are:
* file_format: parses a file path and returns the format of the file.
* compression_formats: supported compressed archive formats for storing DataFrames.
* compress: write data into a compressed archive.
* uncompress: read data from a compressed archive.
* df_formats: supported formats for serializing DataFrames
* df_write: serialize a DataFrame
* df_read: deserialize a DataFrame
