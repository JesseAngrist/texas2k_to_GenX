using CSV
using DataFrames

const SCALE_FACTOR = 1.04
const DEMAND_CSV = "../case/inputs/system/Demand_data.csv"

df = CSV.read(DEMAND_CSV, DataFrame)

demand_cols = [c for c in names(df) if startswith(string(c), "Demand_MW_")]

for col in demand_cols
    df[!, col] = round.(df[!, col] .* SCALE_FACTOR, digits=2)
end

CSV.write(DEMAND_CSV, df)
println("Scaled $(length(demand_cols)) demand columns by $SCALE_FACTOR → $DEMAND_CSV")
