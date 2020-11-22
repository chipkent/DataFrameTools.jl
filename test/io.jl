
dir = mktempdir()

@test :xyz == file_format("/tmp/file.xyz")

txt = "ABC XYZ"
zip = joinpath(dir, "test.zip")

compress(zip, "file.txt") do io
    write(io, txt)
end

uncompress(zip) do io
    @test txt == read(io, String)
end

bad = joinpath(dir, "test.bad")

@test_throws ErrorException compress(bad, "file.txt") do io
end

@test_throws ErrorException uncompress(bad) do io
end

df = DataFrame(A = 1:4, B = ["M", "F", "F", "M"])
formats = [:zip, :csv, :ser, :jld2, :jld2c, :feather, :arrow, :arrow_lz4, :arrow_zstd, :jdf, :parquet]

for format in formats
    file = joinpath(dir, "test_df."*String(format))
    df_write(file, df)
    df2 = df_read(file)
    @test df == df2
end

@test_throws ErrorException df_write(bad, df)
@test_throws ErrorException df_read(file)
