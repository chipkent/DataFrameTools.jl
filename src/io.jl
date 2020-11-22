
using Logging
using ZipFile
using CSV
using JLD2
using DataFrames
using Feather
using Arrow
using Parquet
using ParquetFiles
using JDF
using FstFileFormat

export fileformat, df_write, df_read

"""
Returns the file format from the file suffix.
"""
file_format(file::AbstractString)::Symbol = Symbol(splitext(file)[2][2:end])

"""
Writes a DataFrame to a file.  The file suffix determines how the DataFrame is serialized.
"""
function df_write!(file::AbstractString, df::DataFrame)
    format = file_format(file)
    @debug "BEGIN df_write: $format $file"
    t = time()

    #TODO zip anything??? / unzip anything???
    if format == :zip
        zfiles = ZipFile.Writer(file)
        local f = ZipFile.addfile(dir, "df.csv")
        CSV.write(f, df)
        close(zfiles)
    else format == :csv
        CSV.write(file, df)
    elseif format == :ser
        serialize(file, df)
    elseif format == :jld2
        JLD2.save(file, Dict("df" => df));
    elseif format == :feather
        Feather.write(file, df)
    elseif format == :arrow
        Arrow.write(file, df)
    elseif format == :arrow_lz4
        Arrow.write(file, df; compress=:lz4)
    elseif format == :arrow_zstd
        Arrow.write(file, df; compress=:zstd)
    elseif format == :parquet
        Parquet.write_parquet(file, df)
    elseif format == :jdf
        JDF.savejdf(file, df);
    elseif format == :fst
        FstFileFormat.write(df, file)
    else
        throw(ErrorException("Unsupported dataframe format: format=$format"))
    end

    @debug "END df_write: $format $(time() - t)"
end

"""
Reads a DataFrame from a file.  The file suffix determines how the DataFrame is deserialized.
"""
function df_read(file::AbstractString; dates_as_strings::Bool=true)::DataFrame
    format = file_format(file)

    @debug "BEGIN df_read $format: $file"
    t = time()

    if format == :zip
        zfiles = ZipFile.Reader(file)

        if size(zfiles.files) != 1
            throw(ErrorException("Zip file does not contain exactly one file: zipfile=$file contents=$(zfiles.files)"))
        end

        zf = zfiles.files[1]

        if dates_as_strings
            df = CSV.File(read(zf); dateformat="---DON'T PARSE DATES---") |> DataFrame
        else
            df = CSV.read(zf, DataFrame)
        end

        close(zfiles)
    elseif format == :csv
        if dates_as_strings
            df = CSV.File(read(file); dateformat="---DON'T PARSE DATES---") |> DataFrame
        else
            df = CSV.read(file, DataFrame)
        end
    elseif format == :ser
        df = deserialize(file)
    elseif format == :jld2
        df = JLD2.load(file)["df"]
    elseif format == :feather
        df = Feather.read(file)
    elseif format == :arrow  || format == :arrow_lz4 || format == :arrow_zstd
        df = DataFrame(Arrow.Table(file))
    if format == :parquet
        df = ParquetFiles.load(file) |> DataFrame
    elseif format == :jdf
        df = JDF.loadjdf(file, df);
    elseif format == :fst
        df = FstFileFormat.read(file)
    else
        throw(ErrorException("Unsupported dataframe format: format=$format"))
    end

    #TODO move out???
    #TODO move to write?
    # # feather can't handle all missing
    for n in names(df)
        if eltype(df[!,n]) == Missing
            #TODO remove print
            println("Removed $n for Feather.jl")
            #TODO make optional
            df[!,n] = Vector{Union{Missing, String}}(missing, size(df, 1))
        end
    end

    @debug "END df_read $format: $name $(time() - t)"
    return df
end
