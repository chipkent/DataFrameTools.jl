
using Documenter, DataFrameTools

makedocs(
    modules = [DataFrameTools],
    sitename="DataFrameTools.jl", 
    authors = "Chip Kent",
    format = Documenter.HTML(),
)

deploydocs(
    repo = "github.com/chipkent/DataFrameTools.jl.git", 
    devbranch = "main",
    push_preview = true,
)
