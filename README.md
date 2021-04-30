![Test](https://github.com/chipkent/DataFrameTools.jl/actions/workflows/test.yml/badge.svg)
![Register](https://github.com/chipkent/DataFrameTools.jl/actions/workflows/register.yml/badge.svg)
![Document](https://github.com/chipkent/DataFrameTools.jl/actions/workflows/document.yml/badge.svg)
![Compat Helper](https://github.com/chipkent/DataFrameTools.jl/actions/workflows/compathelper.yml/badge.svg)
![Tagbot](https://github.com/chipkent/DataFrameTools.jl/actions/workflows/tagbot.yml/badge.svg)

# DataFrameTools.jl
Julia package containing utility functions for working with DataFrames.
For example, DataFrameTools supports serialization and deserialization of
DataFrames into many formats.  This allows the DataFrame serialization format to be
easily changed to achieve smaller file sizes, faster file reads, or compatibility with
other applications.

Key functions are:
* <code>file_format</code>: parses a file path and returns the format of the file.
* <code>compression_formats</code>: supported compressed archive formats for storing DataFrames.
* <code>compress</code>: write data into a compressed archive.
* <code>uncompress</code>: read data from a compressed archive.
* <code>df_formats</code>: supported formats for serializing DataFrames
* <code>df_write</code>: serialize a DataFrame
* <code>df_read</code>: deserialize a DataFrame

## Documentation

See [https://chipkent.github.io/DataFrameTools.jl/dev/](https://chipkent.github.io/DataFrameTools.jl/dev/).

Pull requests will publish documentation to <code>https://chipkent.github.io/DataFrameTools.jl/previews/PR##</code>.