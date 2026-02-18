using CSV
using DataFrames
using Bijections
using Distances
using Serialization


using Pkg
Pkg.add(["GeoDataFrames", "ArchGDAL", "XLSX"])
import GeoDataFrames as GDF
using ArchGDAL
using XLSX

function make_load_time_series(load_dir_path::String = "../ERCOT_Load")
    
    # reads CSVs titled #_25 where # is the month of load data each file contains   
    # combines the data into a single DataFrame with the same shared columns as the original CSVs
    
    load_df = DataFrame()

    for month in 1:12
        # print an error message if the month doesn't exist and continue to the next month
        if !isfile(joinpath(load_dir_path, string(month, "_25.csv")))
            @warn "File for month $month not found. Skipping."
            continue
        end
        
        month_df = CSV.read(joinpath(load_dir_path, string(month, "_25.csv")), DataFrame)
        load_df = vcat(load_df, month_df)

    end

    # write the load time series to "master.csv" in the same directory as the original CSVs
    CSV.write(joinpath(load_dir_path, "master.csv"), load_df)
end

function get_load_factors(load_dir_path::String = "../ERCOT_Load")
    
    # reads the master load time series CSV and computes the load factors for each zone
    load_df = CSV.read(joinpath(load_dir_path, "master.csv"), DataFrame)
    
    # find the hour of maximum load for the whole system (:system_total)
    max_load_hour = findmax(load_df.system_total)[2]
    # get the row of the last load hour:
    max_load_row = load_df[max_load_hour, :]

    load_factors_df = copy(load_df)


    # for each row in the load factors df, divide each column by the corresponding column in the max load row
    for i in 1:nrow(load_factors_df)
        for j in 5:ncol(load_factors_df) # ignore the four non-load columns
            load_factors_df[i, j] = load_factors_df[i, j] / max_load_row[j]
        end
    end
    
    # select only the columns for the load factors and the timestamp
    select!(load_factors_df, 5:ncol(load_factors_df))

    # write a Time_Index column that is the index of the row (starting at 1) and move it to the front of the DataFrame
    load_factors_df.Time_Index = 1:nrow(load_factors_df)

    # write the load factors to "master_load_factors.csv" in the same directory as the original CSVs
    CSV.write(joinpath(load_dir_path, "master_load_factors.csv"), load_factors_df)
end

function write_demand_data(load_dir_path::String = "../ERCOT_Load")
    
    # write master.csv and master_load_factors.csv to the load directory if they don't already exist
    if !isfile(joinpath(load_dir_path, "master.csv"))
        make_load_time_series(load_dir_path)
    end
    if !isfile(joinpath(load_dir_path, "master_load_factors.csv"))
        get_load_factors(load_dir_path)
    end

    bus_node_bij = deserialize("busToGenXNode.jls")
    # bus_node_bij[bus_number] = node_number
    # bus_node_bij(node_number) = bus_number

    # load the master load factors CSV first to get the number of timesteps
    load_factors_df = CSV.read(joinpath(load_dir_path, "master_load_factors.csv"), DataFrame)
    n_timesteps = nrow(load_factors_df)

    # create dmd_df with columns Time_Index, Demand_MW_z* where * is each NODE number from bus_node_bij,
    # Voll, Demand_Segment, Cost_of_Demand_Curtailment_per_MW, Max_Demand_Curtailment, Rep_Periods, Timesteps_per_Rep_Period, Sub_Weights
    dmd_df = DataFrame(Time_Index = load_factors_df.Time_Index)
    for bus_number in keys(bus_node_bij)
        node_number = bus_node_bij[bus_number]
        dmd_df[!, Meta.parse("Demand_MW_z$node_number")] = zeros(Float64, n_timesteps)
    end
    # Initialize metadata columns with missing values (will only populate row 1)
    dmd_df[!, :Voll] = Vector{Union{Missing, Float64}}(missing, n_timesteps)
    dmd_df[!, :Demand_Segment] = Vector{Union{Missing, Int}}(missing, n_timesteps)
    dmd_df[!, :Cost_of_Demand_Curtailment_per_MW] = Vector{Union{Missing, Float64}}(missing, n_timesteps)
    dmd_df[!, :Max_Demand_Curtailment] = Vector{Union{Missing, Float64}}(missing, n_timesteps)
    dmd_df[!, :Rep_Periods] = Vector{Union{Missing, Int}}(missing, n_timesteps)
    dmd_df[!, :Timesteps_per_Rep_Period] = Vector{Union{Missing, Int}}(missing, n_timesteps)
    dmd_df[!, :Sub_Weights] = Vector{Union{Missing, Float64}}(missing, n_timesteps)
    
    # ------- first, we handle the demand time series data -------

    # read buses.csv to get bus coords
    buses_df = CSV.read("../TAMU_data/buses.csv", DataFrame)

    # read loads.csv to get load MW values (skip first row which is a title)
    loads_df = CSV.read("../TAMU_data/loads.csv", DataFrame, header=2)

    # load texas county shapes from ../ERCOT_Load/counties/
    counties_gdf = GDF.read(joinpath("../ERCOT_Load/counties/", "Texas_County_Boundaries_Detailed.shp"))
    # print(counties_gdf) 

    source_crs = ArchGDAL.importEPSG(4326) # Lat/Lon
    target_crs = ArchGDAL.getspatialref(counties_gdf.geometry[1]) # The shapefile's CRS

    # imports county-to-weatherzone table (received from ERCOT info request)
    county_to_weather = XLSX.readtable("../ERCOT_Load/cnty_to_weather.xlsx", "Sheet1") |> DataFrame
    # rename columns and entries to lowercase with underscores instead of spaces for easier processing:
    county_to_weather.COUNTY = lowercase.(replace.(county_to_weather.COUNTY, " County" => "")) # remove " County" from the county names and make them lowercase
    # rename county_to_weather columns names to lowercase with underscores:
    rename!(county_to_weather, [n => lowercase(replace(string(n), " " => "_")) for n in names(county_to_weather)])

    for bus_number in keys(bus_node_bij)
        node_number = bus_node_bij[bus_number]
        # print("Bus $bus_number is in county $county_name")

        longitude = buses_df[buses_df.Number .== bus_number, :Longitude][1]
        latitude = buses_df[buses_df.Number .== bus_number, :Latitude][1]

        # create a point geometry for the bus coordinates
        point = ArchGDAL.createpoint(latitude, longitude) # not my preferred coord order, but this is what ArchGDAL expects for some reason
        
        ArchGDAL.createcoordtrans(source_crs, target_crs) do transform
            ArchGDAL.transform!(point, transform)
        end

        # find the index of the county that contains the point geometry
        idx = findfirst(geo -> ArchGDAL.contains(geo, point), counties_gdf.geometry)
        
        weather_zone = "" # the weather zone of the bus

        if isnothing(idx)
            weather_zone = "system_total" # if we can't find the county, let the weather zone be "system_total"
        else
            cnty_nm = lowercase(counties_gdf[idx, :CNTY_NM])
            # find matching weather zone for this county
            matching_rows = county_to_weather[county_to_weather.county .== cnty_nm, :weather_zone]
            if isempty(matching_rows)
                @warn "County '$cnty_nm' not found in weather zone mapping for bus $bus_number, using system_total"
                weather_zone = "system_total"
            else
                weather_zone = matching_rows[1]
                print("Bus number $bus_number is in weather zone $weather_zone")
            end
        end

        # rename weather_zone to lowercase with underscores:
        weather_zone = lowercase(replace(string(weather_zone), " " => "_"))

        # get the load scalar (real power MW) for this bus from loads_df
        matching_loads = loads_df[loads_df[!, Symbol("Number of Bus")] .== bus_number, :MW]

        # handle empty/missing load values by setting to zero
        if isempty(matching_loads)
            load_scalar = 0.0
            @warn "Bus $bus_number has no load entry in loads.csv, setting demand to zero"
        else
            load_value = matching_loads[1]
            if ismissing(load_value) || isnothing(load_value) || (isa(load_value, String) && isempty(strip(load_value)))
                load_scalar = 0.0
                @warn "Bus $bus_number has missing/empty load value, setting demand to zero"
            else
                load_scalar = Float64(load_value)
            end
        end

        # get the load factors column for this weather zone and multiply by the load scalar
        demand_time_series = load_factors_df[!, weather_zone] .* load_scalar

        # insert the demand time series into dmd_df at the appropriate column
        dmd_df[!, Meta.parse("Demand_MW_z$node_number")] = demand_time_series
    end

    # ------- now, we populate the metadata columns for demand curtailment and temporal parameters -------

    # Set temporal and demand curtailment parameters (only first row needs these values)
    # Using a single demand curtailment segment for simplicity
    dmd_df[1, :Voll] = 9000.0  # Value of lost load in $/MWh (standard value)
    dmd_df[1, :Demand_Segment] = 1  # Single demand curtailment segment
    dmd_df[1, :Cost_of_Demand_Curtailment_per_MW] = 1.0  # 100% of VOLL (no discount)
    dmd_df[1, :Max_Demand_Curtailment] = 1.0  # Allow up to 100% demand curtailment
    dmd_df[1, :Rep_Periods] = 1  # 1 representative period (full year, no time domain reduction)
    dmd_df[1, :Timesteps_per_Rep_Period] = n_timesteps  # 8760 hours per year
    dmd_df[1, :Sub_Weights] = n_timesteps  # Each timestep represents itself (no aggregation)

    # Round all demand values to 2 decimal places for cleaner output
    for col in names(dmd_df)
        if startswith(string(col), "Demand_MW_z")
            dmd_df[!, col] = round.(dmd_df[!, col], digits=2)
        end
    end

    # Create output directory if it doesn't exist
    output_dir = "../case/inputs/system"
    mkpath(output_dir)

    # Sort columns: keep metadata columns in standard order, sort Demand_MW_z* columns by zone number
    metadata_cols = [:Voll, :Demand_Segment, :Cost_of_Demand_Curtailment_per_MW, :Max_Demand_Curtailment,
                     :Rep_Periods, :Timesteps_per_Rep_Period, :Sub_Weights, :Time_Index]

    # Get all Demand_MW_z* column names and sort them by zone number
    demand_cols = [name for name in names(dmd_df) if startswith(string(name), "Demand_MW_z")]
    # Sort by extracting the zone number from each column name
    sort!(demand_cols, by = x -> parse(Int, replace(string(x), "Demand_MW_z" => "")))

    # Reorder the DataFrame columns
    dmd_df = select(dmd_df, metadata_cols..., demand_cols...)

    # Write the demand data to CSV
    CSV.write(joinpath(output_dir, "Demand_data.csv"), dmd_df)

    println("\nDemand data successfully written to $output_dir/Demand_data.csv")
    println("Total timesteps: $n_timesteps")
    println("Number of zones: $(length(keys(bus_node_bij)))")
end

write_demand_data()