using Pkg
Pkg.add(["CSV", "DataFrames", "Bijections", "Distances", "Serialization"])

using CSV
using DataFrames
using Bijections
using Distances
using Serialization

# read branches.csv and buses.csv from inputs:
function read_system_data(inputs::String=joinpath(@__DIR__, "..", "TAMU_data"))
    branches_path = joinpath(inputs, "branches.csv")
    buses_path = joinpath(inputs, "buses.csv")
    # error handling if files don't exist
    if !isfile(branches_path)
        error("branches.csv not found in $inputs")
    end
    if !isfile(buses_path)
        error("buses.csv not found in $inputs")
    end
    
    branches_df = CSV.read(branches_path, DataFrame, normalizenames=true)
    buses_df = CSV.read(buses_path, DataFrame, normalizenames=true)
    
    return branches_df, buses_df
end

# writes Network.csv as a list of lines (as opposed to a flow matrix)
function write_networkCSV(input_path::String=joinpath(@__DIR__, "..", "TAMU_data"))
    
    # open ../inputs/system/Network.csv, and write it if it doesn't exist
    network_path = joinpath(@__DIR__, "..", "case", "inputs", "system", "Network.csv")
    if !isfile(network_path)
        println("Network.csv not found, writing Network.csv...")
        network_df = DataFrame(Network_Lines = Integer[],
                               Start_Zone = Integer[],
                               End_Zone = Integer[], 
                               Line_Max_Flow_MW = Float64[], 
                               Line_Max_Reinforcement_MW = Float64[],
                               Line_Reinforcement_Cost_per_MWyr = Float64[], 
                               Line_Loss_Percentage = Float16[], 
                               Line_Voltage_kV = Integer[], 
                               Line_Reactance_Ohms = Float64[])
    else # TODO: rethink control flow here...
        network_df = CSV.read(network_path, DataFrame, normalizenames=true)
    end

    # read branches and buses data
    branches_df, buses_df = read_system_data(input_path)

    # rename columns to all lowercase with underscores instead of spaces:
    function clean_colnames!(df::DataFrame)
        # Get the current column names as a vector of Strings
        current_names = names(df)
        
        # process the names: lowercase and replace spaces with underscores
        new_names = [replace(lowercase(name), " " => "_") for name in current_names]
        
        # Rename the columns in place
        rename!(df, new_names)
        
        return df
    end

    clean_colnames!(branches_df)
    clean_colnames!(buses_df)   

    # print(first(branches_df,5))
    # print(first(buses_df,5))

    # create bus number to zone number bijection:
    include(joinpath(@__DIR__, "map_buses_to_GenX_labels.jl"))
    bus_node_bij = deserialize("busToGenXLabel.jls")
    # bus_node_bij[bus_number] = node_number
    # bus_node_bij(node_number) = bus_number

    # identify lines (non-transformer branches)
    lines_df = branches_df[branches_df.branch_device_type .== "Line", :]

    for (line_number, line) in enumerate(eachrow(lines_df))
        from_bus = line[:from_number]
        to_bus = line[:to_number]

        pu_resistance = line[:r]
        pu_reactance = line[:x]

        nom_voltage = line[:to_nom_kv] # NOT PER UNIT VOLTAGE
        pu_voltage = 1 # OPF assumption

        max_flow = line[:lim_mva_a] # taking MVA limit to be real power limit

        # calculate Line_Reinforcement_Cost_per_MWyr:

        # calculate line length 
        from_lat = buses_df[buses_df.number .== from_bus, :latitude]
        from_long = buses_df[buses_df.number .== from_bus, :longitude]
        to_lat = buses_df[buses_df.number .== to_bus, :latitude]
        to_long = buses_df[buses_df.number .== to_bus, :longitude]

        from_coords = (from_long[1], from_lat[1])
        to_coords = (to_long[1], to_lat[1])

        # calculates haversine distance in meters
        len = evaluate(Haversine(), from_coords, to_coords)
        len = Haversine()(from_coords, to_coords)

        mi = len / 1609.34 # convert to miles

        cost_per_mile = 10^6 * ((0.00745) * nom_voltage - 0.1252) # in $/mile, 
        # from an R^2 = 0.98 linear fit of the midpoints costs per mile supplied by Claude Sonnet:
        #     138 kV: ~$0.8-1.2M/mile
        #    230 kV: ~$1.2-2.0M/mile
        #   345 kV: ~$2.0-3.0M/mile
        #  500 kV: ~$2.5-4.0M/mile
        # 765 kV: ~$5.2-6.3M/mile

        i = 0.08 # discount rate
        n = 50 # 50 yr investment lifespan
        crf = (i * (1 + i)^n) / ((1 + i)^n - 1) # capital recovery factor for n year life
        # realistically this CRF ~ i, sensitive to discount rate

        reinforcement_cost_per_mwyr = (cost_per_mile * mi * crf) / max_flow # $/MW-yr

        # set max reinforcement to zero for non-candidate lines
        max_reinforcement = 0 * max_flow 

        loss_fraction = pu_resistance # this seems kind of reasonable as a first approximation?
        # but, the source is Claude Sonnet when asked for a reasonable linear approximation for OPF
        # needs fact-checking

        pu_voltage = 1 # TODO: see whether DC_OPF formulation uses per-unit or nominal voltages and whether that's working as intended
        
        # convert bus numbers to zone numbers
        from_zone = bus_node_bij[from_bus]
        to_zone = bus_node_bij[to_bus]

        # recall: ["Network_Lines", "Start_Zone", "End_Zone", 
        # "Line_Max_Flow_MW", "Line_Max_Reinforcement_MW", 
        #"Line_Reinforcement_Cost_per_MWyr", "Line_Loss_Percentage", 
        # "Line_Voltage_kV", "Line_Reactance_Ohms"]

        newRow = (line_number, from_zone, to_zone, max_flow, max_reinforcement,
                            reinforcement_cost_per_mwyr, loss_fraction, 1, pu_reactance)

        # print(length(newRow))

        # compares the length of the new row with the number of columns in the network_df:
        # println("Length of newRow: $(length(newRow)), Number of columns in network_df: $(ncol(network_df))")

        # we use a per-unit voltage of 1 and our per-unit reactance
        push!(network_df, newRow) 
    end

    # write Network.csv
    CSV.write(network_path, network_df)
    println("Network.csv written to $network_path")
end

if abspath(PROGRAM_FILE) == @__FILE__
    write_networkCSV()
end