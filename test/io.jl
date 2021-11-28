using Dates

dir = mktempdir()

@test :xyz == file_format("/tmp/file.xyz")

txt = "ABC XYZ"
zip = joinpath(dir, "test.zip")

compress(zip, "file.txt") do io
    write(io, txt)
end

uncompress(zip) do name, io
    @test name == "file.txt"
    @test txt == read(io, String)
end

bad = joinpath(dir, "test.bad")

@test_throws ErrorException compress(bad, "file.txt") do io
end

@test_throws ErrorException uncompress(bad) do io
end

df = DataFrame(A = 1:4, B = ["M", "F", "F", "M"])


for format in df_formats()
    file = joinpath(dir, "test_df."*String(format))
    df_write(file, df; subformat=:csv)
    df2 = df_read(file)
    @test df == df2
end


for format in compression_formats()
    file = joinpath(dir, "test_df."*String(format))

    for subformat in df_formats()
        if subformat == :jld2 || subformat == :jld2c || subformat == :parquet || subformat == :jdf
            @test_throws ErrorException df_write(file, df; subformat=subformat)
            continue
        end

        df_write(file, df; subformat=subformat)
        df2 = df_read(file)
        @test df == df2
    end
end


@test_throws ErrorException df_write(bad, df)
@test_throws ErrorException df_read(bad)

# Test Issue #14
df = DataFrame()
df[!, :test] = [DateTime(2000,1,1,1,1,1), missing]
DataFrameTools.df_write(joinpath(dir, "test.jdf"), df)

# Test PR #15
using Pipe
df = DataFrame()
df[!, :test] = [DateTime(2000,1,1,1,1,1), missing]
in_file = joinpath(dir, "test.feather")
out_file = joinpath(dir, "test2.feather")
df_write(in_file, df)
pipe_it(in_file, out_file) = @pipe df_read(in_file) |> DataFrame |> df_write(out_file, _)
pipe_it(in_file, out_file)
