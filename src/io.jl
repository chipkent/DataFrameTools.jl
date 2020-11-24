
using Logging
using DataFrames
using ZipFile
using CSV
using Serialization
using JLD2
using Feather
using Arrow
using Parquet
using JDF

export file_format, compression_formats, compress, uncompress, df_formats, df_write, df_read

"""
Returns the file format from the file suffix.
"""
file_format(file::AbstractString)::Symbol = Symbol(splitext(file)[2][2:end])


"""
Returns the file formats supported for compressed archives.
"""
compression_formats()::Vector{Symbol} = [:zip]


"""
Creates a compressed file.  The type of compression is inferred from the
output file name.  subfile is the name of the file within the compressed
archive.  f is a function which writes to the archive.

The typical use case is:

    compress(file, subfile) do io
        # put here the body of function f(io) to write to io
    end

"""
function compress(f::Function, file::AbstractString, subfile::AbstractString)
    format = file_format(file)
    @debug "BEGIN compress: $format $file"
    t = time()

    if format == :zip
        local zfiles = ZipFile.Writer(file)

        try
            local zf = ZipFile.addfile(zfiles, subfile)

            try
                f(zf)
            finally
                close(zf)
            end
        finally
            close(zfiles)
        end
    else
        throw(ErrorException("Unsupported compression format: format=$format"))
    end

    @debug "END compress: $format $(time() - t)"
end


"""
Uncompresses a compressed file.  The type of compression is inferred from the
output file name.  f is a function which reads from the archive.

The typical use case is:

    uncompress(file) do io
        # put here the body of function f(io) to write to io
    end

"""
function uncompress(f::Function, file::AbstractString)
    format = file_format(file)
    @debug "BEGIN uncompress: $format $file"
    t = time()

    if format == :zip

        zfiles = ZipFile.Reader(file)

        try
            if size(zfiles.files,1) != 1
                throw(ErrorException("Zip file does not contain exactly one file: zipfile=$file contents=$(zfiles.files)"))
            end

            zf = zfiles.files[1]

            try
                f(zf.name, zf)
            finally
                close(zf)
            end
        finally
            close(zfiles)
        end
    else
        throw(ErrorException("Unsupported compression format: format=$format"))
    end

    @debug "END uncompress: $format $(time() - t)"
end


"""
Returns the file formats supported for DataFrames.
"""
df_formats()::Vector{Symbol} = [:csv, :ser, :jld2, :jld2c, :feather, :arrow, :arrow_lz4, :arrow_zstd, :parquet, :jdf]


"""
Writes a DataFrame to a file.  The file suffix determines how the DataFrame is serialized.
If the file has a compressed suffix, subformat determines how the DataFrame
is serialized in the compressed archive.
"""
function df_write(file::AbstractString, df::DataFrame; subformat::Union{Nothing,Symbol}=nothing)
    format = file_format(file)

    if in(format, compression_formats())
        if subformat == nothing
            throw(ErrorException("Compressed archives must specify the DataFrame serialization format (subformat)."))
        end

        subfile = "db." * String(subformat)

        compress(file, subfile) do f
            _df_write(f, subformat, df)
        end

        return
    end

    _df_write(file, format, df)
end


"""
Writes a DataFrame.  File can be a file path or an IO.
"""
function _df_write(file, format::Symbol, df::DataFrame)

    @debug "BEGIN df_write: $format $file"
    t = time()

    if format == :csv
        CSV.write(file, df)
    elseif format == :ser
        serialize(file, df)
    elseif format == :jld2
        if !isa(file, AbstractString)
            throw(ErrorException("JLD2 is not supported in compressed archives."))
        end

        JLD2.@save file df=df
    elseif format == :jld2c
        if !isa(file, AbstractString)
            throw(ErrorException("JLD2 is not supported in compressed archives."))
        end

        JLD2.@save file {compress=true} df=df
    elseif format == :feather
        Feather.write(file, df)
    elseif format == :arrow
        Arrow.write(file, df)
    elseif format == :arrow_lz4
        Arrow.write(file, df; compress=:lz4)
    elseif format == :arrow_zstd
        Arrow.write(file, df; compress=:zstd)
    elseif format == :parquet
        if !isa(file, AbstractString)
            throw(ErrorException("Parquet is not supported in compressed archives."))
        end

        Parquet.write_parquet(file, df)
    elseif format == :jdf
        if !isa(file, AbstractString)
            throw(ErrorException("JDF is not supported in compressed archives."))
        end

        JDF.savejdf(file, df)
    else
        throw(ErrorException("Unsupported dataframe format: format=$format"))
    end

    @debug "END df_write: $format $(time() - t)"
end


"""
Reads a DataFrame from a file.  The file suffix determines how the DataFrame is deserialized.
"""
function df_read(file::AbstractString; dates_as_strings::Bool=true, missing_type::Type=String, missing_types::Dict{String,Type}=Dict{String,Type}())::DataFrame
    format = file_format(file)

    if in(format, compression_formats())
        local df

        uncompress(file) do name, f
            format = file_format(name)
            df = _df_read(f, format; dates_as_strings=dates_as_strings, missing_type=missing_type, missing_types=missing_types)
        end

        return df
    end

    return _df_read(file, format; dates_as_strings=dates_as_strings, missing_type=missing_type, missing_types=missing_types)
end


"""
Reads a DataFrame.  File can be a file path or an IO.
"""
function _df_read(file, format::Symbol; dates_as_strings::Bool=true, missing_type::Type=String, missing_types::Dict{String,Type}=Dict{String,Type}())::DataFrame
    @debug "BEGIN df_read $format: $file"
    t = time()

    if format == :csv
        if dates_as_strings
            df = CSV.File(read(file); dateformat="---DON'T PARSE DATES---") |> DataFrame
        else
            df = CSV.read(file, DataFrame)
        end
    elseif format == :ser
        df = deserialize(file)
    elseif format == :jld2 || format == :jld2c
        if !isa(file, AbstractString)
            throw(ErrorException("JLD2 is not supported in compressed archives."))
        end

        JLD2.@load file df
    elseif format == :feather
        df = Feather.read(file)
    elseif format == :arrow  || format == :arrow_lz4 || format == :arrow_zstd
        df = DataFrame(Arrow.Table(file))
    elseif format == :parquet
        if !isa(file, AbstractString)
            throw(ErrorException("Parquet is not supported in compressed archives."))
        end

        parquetfile = Parquet.File(file)

        try
            local cc = BatchedColumnsCursor(parquetfile)
            local batchvals, state = iterate(cc)

            df = DataFrame()

            for key in keys(batchvals)
                df[!,key] = batchvals[key]
            end

        finally
            close(parquetfile)
        end

    elseif format == :jdf
        if !isa(file, AbstractString)
            throw(ErrorException("JDF is not supported in compressed archives."))
        end

        df = JDF.loadjdf(file);
    else
        throw(ErrorException("Unsupported dataframe format: format=$format"))
    end

    # Some serialization methods can not handle columns of type Missing.
    for n in names(df)
        if eltype(df[!,n]) == Missing
            T = haskey(missing_types, n) ? missing_types[n] : missing_type
            @debug "Column with all missing values retyped as Vector{Union{Missing, $T}}: name=$n"
            df[!,n] = Vector{Union{Missing, T}}(missing, size(df, 1))
        end
    end

    @debug "END df_read $format: $name $(time() - t)"
    return df
end
