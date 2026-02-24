using CSV
using DataFrames

function write_fuels_dataCSV()
    fuels_data_path = joinpath(@__DIR__, "..", "case", "system", "Fuels_data.csv")
    master_path = joinpath(@__DIR__, "..", "ERCOT_Load", "master.csv")
    n_timesteps = nrow(CSV.read(master_path, DataFrame))
    
    fuels_df = DataFrame(Time_index = Integer[], TX_NG = Float64[], TX_coal = Float64[], TX_oil = Float64[], Nuclear = Float64[])

    # see EIA: https://www.eia.gov/environment/emissions/co2_vol_mass.php
    NG_EMISSIONS = 0.053 # metric tons CO2 per MMBtu
    COAL_EMISSIONS = 0.096 # metric tons CO2 per MMBtu,
    OIL_EMISSIONS = 0.074 # metric tons CO2 per MMBtu
    NUCLEAR_EMISSIONS = 0.0 # nuclear produces no direct CO2

    emissionsRow = (0, NG_EMISSIONS, COAL_EMISSIONS, OIL_EMISSIONS, NUCLEAR_EMISSIONS)
    
    push!(fuels_df, emissionsRow)

    # prices from reference case of AEO for 2025: https://www.eia.gov/outlooks/aeo/data/browser/#/?id=1-AEO2025&region=0-0&cases=ref2025~hm2025~lm2025~highprice~lowprice~highogs~lowogs~highZTC~lowZTC~nocaa111~alttrnp~aeo2023ref&start=2023&end=2050&f=Q&linechart=ref2025-d032025a.3-1-AEO2025~hm2025-d032025a.3-1-AEO2025~lm2025-d032425b.3-1-AEO2025~highprice-d032525b.3-1-AEO2025~lowprice-d032125a.3-1-AEO2025~highogs-d032425b.3-1-AEO2025~lowogs-d032625c.3-1-AEO2025~highZTC-d032125b.3-1-AEO2025~lowZTC-d032425a.3-1-AEO2025~nocaa111-d032525c.3-1-AEO2025~alttrnp-d032125a.3-1-AEO2025~aeo2023ref-d020623a.3-1-AEO2025&sourcekey=0 
    NG_PRICE_USDperMMBtu = 2.88
    COAL_PRICE_USD_perMMBtu = 2.79
    OIL_PRICE_USD_perMMBtu = round(67.73 / 5.7, digits=2) # West Texas spot price divided by 5.7 MMBtu/barrel
    # round oil pric
    # Nuclear fuel (enriched uranium): ~$0.71/MMBtu equivalent (EIA AEO 2025 reference)
    NUCLEAR_PRICE_USDperMMBtu = 0.71

    for i in 1:n_timesteps
        priceRow = (i, NG_PRICE_USDperMMBtu, COAL_PRICE_USD_perMMBtu, OIL_PRICE_USD_perMMBtu, NUCLEAR_PRICE_USDperMMBtu)
        push!(fuels_df, priceRow)
    end

    CSV.write(fuels_data_path, fuels_df)
    println("Fuels_data.csv written to $fuels_data_path")
end

if abspath(PROGRAM_FILE) == @__FILE__
    write_fuels_dataCSV()
end