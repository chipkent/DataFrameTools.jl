
using DataFrameTools

dir = mktempdir()

@test :xyz file_format("/tmp/file.xyz")

txt = "ABC XYZ"
zip = joinpath(dir, "test.zip")

compress(zip, "file.txt") do io
    write(io, txt)
end

uncompress(zip) do io
    @test txt read(io, String)
end

bad = joinpath(dir, "test.bad")

@test_throws ErrorException compress(bad, "file.txt") do io
    write(io, txt)
end

@test_throws ErrorException uncompress(bad) do io
    @test txt read(io, String)
end
